#!/usr/bin/env python3
"""
Azure Batch Upload Script  
Uploads processing results for any batch to Azure Blob Storage
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
        logging.FileHandler('/root/autodl-tmp/azure_sync/logs/upload.log')
    ]
)
logger = logging.getLogger(__name__)


class AzureBatchUploader:
    """Upload processing results for any batch to Azure Blob Storage"""
    
    def __init__(self, connection_string: str):
        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container = "videos"
        self.base_results_path = "ruijian-research/batch_results"
    
    def upload_batch_results(self, batch_id: str, results_dir: str, limit: int = None, start_from: str = None) -> Dict:
        """Upload all processing results for a batch"""
        
        results_path = Path(results_dir)
        if not results_path.exists():
            raise FileNotFoundError(f"Results directory not found: {results_path}")
        
        # Find all video result directories
        video_dirs = []
        for item in results_path.iterdir():
            if item.is_dir() and item.name.startswith('00000500'):
                video_dirs.append(item)
        
        video_dirs.sort()
        
        # Filter results if needed
        if start_from:
            start_idx = 0
            for i, video_dir in enumerate(video_dirs):
                if start_from in video_dir.name:
                    start_idx = i
                    break
            video_dirs = video_dirs[start_idx:]
            logger.info(f"⏭️ Starting from video: {start_from}")
        
        if limit:
            video_dirs = video_dirs[:limit]
            logger.info(f"🔢 Limited to {limit} video results")
        
        logger.info(f"🚀 Uploading {len(video_dirs)} video results for batch_{batch_id}")
        
        # Upload statistics
        uploaded_count = 0
        failed_count = 0
        total_files = 0
        total_size = 0
        start_time = time.time()
        
        for i, video_dir in enumerate(video_dirs):
            video_id = video_dir.name
            
            try:
                logger.info(f"☁️ [{i+1}/{len(video_dirs)}] Uploading {video_id}")
                
                # Upload all files in this video directory
                files_uploaded, files_size = self._upload_video_results(batch_id, video_id, video_dir)
                
                if files_uploaded > 0:
                    uploaded_count += 1
                    total_files += files_uploaded
                    total_size += files_size
                    logger.info(f"✅ Uploaded {video_id} ({files_uploaded} files, {files_size:,} bytes)")
                else:
                    failed_count += 1
                    logger.error(f"❌ Failed to upload {video_id}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"💥 Error uploading {video_id}: {e}")
        
        # Summary statistics
        elapsed_time = time.time() - start_time
        success_rate = (uploaded_count / max(1, uploaded_count + failed_count)) * 100
        
        stats = {
            'batch_id': batch_id,
            'total_video_dirs': len(video_dirs),
            'uploaded_videos': uploaded_count,
            'failed_videos': failed_count,
            'success_rate': success_rate,
            'total_files_uploaded': total_files,
            'total_size_mb': total_size / (1024 * 1024),
            'elapsed_time_sec': elapsed_time,
            'results_directory': str(results_path),
            'azure_destination': f"{self.base_results_path}/batch_{batch_id}/"
        }
        
        logger.info(f"""
🎯 Upload Summary for batch_{batch_id}:
   ✅ Successfully uploaded: {uploaded_count} videos
   ❌ Failed: {failed_count} videos
   📊 Success rate: {success_rate:.1f}%
   📁 Total files: {total_files}
   💾 Total size: {total_size / (1024 * 1024):.1f} MB
   ⏱️ Time taken: {elapsed_time:.1f} seconds
   ☁️ Azure path: {stats['azure_destination']}
        """)
        
        # Save upload report
        report_path = results_path / f"upload_report_batch_{batch_id}.json"
        with open(report_path, 'w') as f:
            json.dump(stats, f, indent=2)
        
        return stats
    
    def _upload_video_results(self, batch_id: str, video_id: str, video_dir: Path) -> Tuple[int, int]:
        """Upload all result files for a single video"""
        upload_base = f"{self.base_results_path}/batch_{batch_id}/{video_id}"
        
        files_uploaded = 0
        total_size = 0
        
        # Upload all files maintaining directory structure
        for file_path in video_dir.rglob('*'):
            if file_path.is_file():
                relative_path = file_path.relative_to(video_dir)
                blob_path = f"{upload_base}/{relative_path}"
                
                success, file_size = self._upload_file(file_path, blob_path)
                if success:
                    files_uploaded += 1
                    total_size += file_size
        
        return files_uploaded, total_size
    
    def _upload_file(self, local_path: Path, blob_path: str) -> Tuple[bool, int]:
        """Upload a single file to Azure"""
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container,
                blob=blob_path
            )
            
            # Check if file already exists and is identical
            try:
                blob_properties = blob_client.get_blob_properties()
                local_size = local_path.stat().st_size
                if local_size == blob_properties.size:
                    logger.debug(f"⏭️ Skipping {blob_path} - already exists ({local_size:,} bytes)")
                    return True, local_size
            except ResourceNotFoundError:
                pass  # File doesn't exist, proceed with upload
            
            # Upload file
            file_size = local_path.stat().st_size
            with open(local_path, 'rb') as f:
                blob_client.upload_blob(f, overwrite=True)
            
            logger.debug(f"✅ Uploaded {blob_path} ({file_size:,} bytes)")
            return True, file_size
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {local_path} → {blob_path}: {e}")
            return False, 0
    
    def check_video_completeness(self, results_dir: str) -> Dict:
        """Check completeness of video processing results"""
        results_path = Path(results_dir)
        
        required_files = [
            "part2_output/inpainted_video.mp4",
            "part2_output/masked_area_filled.mp4",
            "part2_output/inpainted_frame.png"
        ]
        
        video_stats = {
            'complete_videos': [],
            'incomplete_videos': [],
            'missing_videos': []
        }
        
        for video_dir in results_path.glob('00000500*'):
            if video_dir.is_dir():
                video_id = video_dir.name
                missing_files = []
                
                for required_file in required_files:
                    file_path = video_dir / required_file
                    if not file_path.exists() or file_path.stat().st_size < 1000:
                        missing_files.append(required_file)
                
                if not missing_files:
                    video_stats['complete_videos'].append(video_id)
                else:
                    video_stats['incomplete_videos'].append({
                        'video_id': video_id,
                        'missing_files': missing_files
                    })
        
        logger.info(f"""
📊 Results Completeness Check:
   ✅ Complete videos: {len(video_stats['complete_videos'])}
   ⚠️ Incomplete videos: {len(video_stats['incomplete_videos'])}
        """)
        
        return video_stats
    
    def upload_only_complete_videos(self, batch_id: str, results_dir: str, limit: int = None) -> Dict:
        """Upload only videos that have all required output files"""
        
        # Check completeness first
        completeness_stats = self.check_video_completeness(results_dir)
        complete_videos = completeness_stats['complete_videos']
        
        if limit:
            complete_videos = complete_videos[:limit]
        
        logger.info(f"🎯 Uploading {len(complete_videos)} complete videos for batch_{batch_id}")
        
        # Upload only complete videos
        results_path = Path(results_dir)
        uploaded_count = 0
        failed_count = 0
        total_files = 0
        total_size = 0
        start_time = time.time()
        
        for i, video_id in enumerate(complete_videos):
            video_dir = results_path / video_id
            
            try:
                logger.info(f"☁️ [{i+1}/{len(complete_videos)}] Uploading {video_id}")
                
                files_uploaded, files_size = self._upload_video_results(batch_id, video_id, video_dir)
                
                if files_uploaded > 0:
                    uploaded_count += 1
                    total_files += files_uploaded
                    total_size += files_size
                    logger.info(f"✅ Uploaded {video_id} ({files_uploaded} files)")
                else:
                    failed_count += 1
                    logger.error(f"❌ Failed to upload {video_id}")
                
            except Exception as e:
                failed_count += 1
                logger.error(f"💥 Error uploading {video_id}: {e}")
        
        # Statistics
        elapsed_time = time.time() - start_time
        success_rate = (uploaded_count / max(1, uploaded_count + failed_count)) * 100
        
        stats = {
            'batch_id': batch_id,
            'complete_videos_found': len(completeness_stats['complete_videos']),
            'incomplete_videos_skipped': len(completeness_stats['incomplete_videos']),
            'uploaded_videos': uploaded_count,
            'failed_videos': failed_count,
            'success_rate': success_rate,
            'total_files_uploaded': total_files,
            'total_size_mb': total_size / (1024 * 1024),
            'elapsed_time_sec': elapsed_time
        }
        
        return stats


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Azure Batch Uploader")
    
    # Batch selection
    parser.add_argument("--batch", required=True, help="Batch ID for upload destination (e.g., 001, 002)")
    parser.add_argument("--results", required=True, help="Local results directory to upload")
    
    # Upload control
    parser.add_argument("--limit", type=int, help="Limit number of videos to upload")
    parser.add_argument("--start-from", help="Video ID to start uploading from")
    parser.add_argument("--complete-only", action="store_true", help="Upload only videos with complete results")
    parser.add_argument("--check-only", action="store_true", help="Only check completeness, don't upload")
    
    # Connection
    parser.add_argument("--connection-string", help="Azure storage connection string (or use .env)")
    
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
        uploader = AzureBatchUploader(connection_string)
        
        # Check completeness only
        if args.check_only:
            stats = uploader.check_video_completeness(args.results)
            logger.info("🔍 Completeness check completed")
            return
        
        # Upload results
        if args.complete_only:
            stats = uploader.upload_only_complete_videos(
                batch_id=args.batch,
                results_dir=args.results,
                limit=args.limit
            )
        else:
            stats = uploader.upload_batch_results(
                batch_id=args.batch,
                results_dir=args.results,
                limit=args.limit,
                start_from=args.start_from
            )
        
        logger.info("🏁 Upload completed successfully!")
        
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()