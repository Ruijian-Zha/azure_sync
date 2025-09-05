#!/usr/bin/env python3
"""
Azure Batch Download Script
Downloads video-image pairs for any batch from Azure Blob Storage
"""

import json
import logging
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, List, Tuple
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/root/autodl-tmp/azure_sync/logs/download.log')
    ]
)
logger = logging.getLogger(__name__)


class AzureBatchDownloader:
    """Download video-image pairs for any batch from Azure Blob Storage"""
    
    def __init__(self, connection_string: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container = "videos"
        self.video_source_path = "ruijian-research/raw"
        self.image_source_path = "ruijian-research/celeba-hq"
    
    def load_batch_config(self, batch_id: str) -> Dict:
        """Load batch configuration from JSON file"""
        batch_config_path = Path(f"/root/autodl-tmp/azure_sync/batch_configs/batch_{batch_id}/video_image_mapping.json")
        
        if not batch_config_path.exists():
            raise FileNotFoundError(f"Batch config not found: {batch_config_path}")
        
        with open(batch_config_path, 'r') as f:
            batch_data = json.load(f)
        
        if 'mapping' not in batch_data:
            raise ValueError(f"Invalid batch file format: missing 'mapping' key")
        
        logger.info(f"📊 Loaded batch_{batch_id}: {len(batch_data['mapping'])} video-image pairs")
        return batch_data
    
    def download_batch(self, batch_id: str, output_dir: str, limit: int = None, start_from: str = None) -> Dict:
        """Download all video-image pairs for a batch"""
        
        # Load batch configuration
        batch_data = self.load_batch_config(batch_id)
        mapping = batch_data['mapping']
        
        # Create output directory
        output_path = Path(output_dir) / f"batch_{batch_id}_data"
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Filter mapping if needed
        video_pairs = list(mapping.items())
        
        if start_from:
            start_idx = 0
            for i, (video_file, _) in enumerate(video_pairs):
                if start_from in video_file:
                    start_idx = i
                    break
            video_pairs = video_pairs[start_idx:]
            logger.info(f"⏭️ Starting from video: {start_from}")
        
        if limit:
            video_pairs = video_pairs[:limit]
            logger.info(f"🔢 Limited to {limit} video pairs")
        
        logger.info(f"🚀 Downloading {len(video_pairs)} video-image pairs for batch_{batch_id}")
        
        # Download statistics
        downloaded_count = 0
        failed_count = 0
        total_size = 0
        start_time = time.time()
        
        for i, (video_file, image_file) in enumerate(video_pairs):
            video_id = video_file.replace('.0_processed.mp4', '')
            
            try:
                logger.info(f"📥 [{i+1}/{len(video_pairs)}] Processing {video_id}")
                
                # Create video-specific directory
                video_output_dir = output_path / video_id
                video_output_dir.mkdir(exist_ok=True)
                
                # Download video
                video_downloaded, video_size = self._download_file(
                    f"{self.video_source_path}/{video_file}",
                    video_output_dir / video_file
                )
                
                # Download reference image
                image_downloaded, image_size = self._download_file(
                    f"{self.image_source_path}/{image_file}",
                    video_output_dir / image_file
                )
                
                if video_downloaded and image_downloaded:
                    downloaded_count += 1
                    total_size += video_size + image_size
                    logger.info(f"✅ Downloaded {video_id} ({video_size + image_size:,} bytes)")
                else:
                    failed_count += 1
                    logger.error(f"❌ Failed to download {video_id}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"💥 Error downloading {video_id}: {e}")
        
        # Summary statistics
        elapsed_time = time.time() - start_time
        success_rate = (downloaded_count / max(1, downloaded_count + failed_count)) * 100
        
        stats = {
            'batch_id': batch_id,
            'total_requested': len(video_pairs),
            'downloaded': downloaded_count,
            'failed': failed_count,
            'success_rate': success_rate,
            'total_size_mb': total_size / (1024 * 1024),
            'elapsed_time_sec': elapsed_time,
            'output_directory': str(output_path)
        }
        
        logger.info(f"""
🎯 Download Summary for batch_{batch_id}:
   ✅ Successfully downloaded: {downloaded_count}
   ❌ Failed: {failed_count}  
   📊 Success rate: {success_rate:.1f}%
   💾 Total size: {total_size / (1024 * 1024):.1f} MB
   ⏱️ Time taken: {elapsed_time:.1f} seconds
   📁 Output: {output_path}
        """)
        
        # Save download report
        report_path = output_path / "download_report.json"
        with open(report_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        return stats
    
    def _download_file(self, blob_path: str, local_path: Path) -> Tuple[bool, int]:
        """Download a single file from Azure"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container,
                blob=blob_path
            )
            
            # Check if file already exists and is complete
            if local_path.exists():
                try:
                    # Get blob size to compare
                    blob_properties = blob_client.get_blob_properties()
                    local_size = local_path.stat().st_size
                    if local_size == blob_properties.size:
                        logger.debug(f"⏭️ Skipping {blob_path} - already exists ({local_size:,} bytes)")
                        return True, local_size
                except Exception:
                    pass  # Download anyway if we can't verify
            
            # Download file
            with open(local_path, 'wb') as f:
                download_stream = blob_client.download_blob()
                data = download_stream.readall()
                f.write(data)
                file_size = len(data)
            
            logger.debug(f"✅ Downloaded {blob_path} ({file_size:,} bytes)")
            return True, file_size
            
        except ResourceNotFoundError:
            logger.error(f"❌ File not found in Azure: {blob_path}")
            return False, 0
        except Exception as e:
            logger.error(f"❌ Failed to download {blob_path}: {e}")
            return False, 0
    
    def list_available_batches(self) -> List[str]:
        """List all available batch configurations"""
        batch_configs_dir = Path("/root/autodl-tmp/azure_sync/batch_configs")
        batches = []
        
        for batch_dir in batch_configs_dir.glob("batch_*"):
            if batch_dir.is_dir():
                batch_id = batch_dir.name.replace("batch_", "")
                mapping_file = batch_dir / "video_image_mapping.json"
                if mapping_file.exists():
                    batches.append(batch_id)
        
        return sorted(batches)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Azure Batch Downloader")
    
    # Batch selection
    parser.add_argument("--batch", required=True, help="Batch ID to download (e.g., 001, 002)")
    parser.add_argument("--output", default="/root/autodl-tmp/azure_downloads", help="Output directory")
    
    # Download control
    parser.add_argument("--limit", type=int, help="Limit number of video pairs to download")
    parser.add_argument("--start-from", help="Video ID to start downloading from")
    
    # Connection
    parser.add_argument("--connection-string", help="Azure storage connection string (or use .env)")
    parser.add_argument("--list-batches", action="store_true", help="List available batch configurations")
    
    args = parser.parse_args()
    
    # Load connection string
    connection_string = args.connection_string
    if not connection_string:
        env_file = Path("/root/autodl-tmp/azure_sync/credentials/.env")
        if env_file.exists():
            with open(env_file, 'r') as f:
                for line in f:
                    if line.strip() and line.startswith('AZURE_STORAGE_CONNECTION_STRING'):
                        connection_string = line.split('=', 1)[1].strip().strip('"').strip("'")
                        break
    
    if not connection_string:
        logger.error("❌ Azure connection string not found. Use --connection-string or set up .env file")
        sys.exit(1)
    
    try:
        downloader = AzureBatchDownloader(connection_string)
        
        # List available batches
        if args.list_batches:
            batches = downloader.list_available_batches()
            logger.info(f"📦 Available batches: {', '.join(batches)}")
            return
        
        # Download batch
        stats = downloader.download_batch(
            batch_id=args.batch,
            output_dir=args.output,
            limit=args.limit,
            start_from=args.start_from
        )
        
        logger.info("🏁 Download completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()