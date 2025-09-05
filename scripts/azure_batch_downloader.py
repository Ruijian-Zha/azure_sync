#!/usr/bin/env python3
"""
Azure Blob Storage Batch Downloader
Downloads video-image pairs from Azure Blob Storage based on batch mapping files
Supports the videos/ruijian-research/raw and videos/ruijian-research/celeba-hq structure
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

class AzureBatchDownloader:
    def __init__(self, connection_string=None):
        """
        Initialize Azure Batch Downloader
        
        Args:
            connection_string (str): Azure Storage connection string
        """
        # Get connection string from parameter or environment
        self.conn_str = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not self.conn_str:
            # Load from .env file
            env_file_path = "/root/autodl-tmp/azure_sync/credentials/.env"
            if os.path.exists(env_file_path):
                with open(env_file_path, 'r') as f:
                    for line in f:
                        if line.strip() and line.startswith('AZURE_STORAGE_CONNECTION_STRING'):
                            self.conn_str = line.split('=', 1)[1].strip().strip('"').strip("'")
                            break
            if not self.conn_str:
                raise ValueError("Azure Storage connection string not found. Please set AZURE_STORAGE_CONNECTION_STRING environment variable or create credentials/.env file")
        
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_name = "videos"
        self.raw_video_path = "ruijian-research/raw"
        self.celeba_path = "ruijian-research/celeba-hq"
    
    def download_file(self, blob_path, local_path):
        """
        Download a single file from Azure Blob Storage
        
        Args:
            blob_path (str): Path to blob in Azure storage
            local_path (str): Local file path to save to
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name,
                blob=blob_path
            )
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            print(f"📥 Downloading {blob_path} -> {local_path}")
            
            with open(local_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            # Get file size for reporting
            file_size = os.path.getsize(local_path)
            print(f"   ✅ Downloaded {file_size:,} bytes")
            
            return True
            
        except ResourceNotFoundError:
            print(f"   ❌ File not found in Azure: {blob_path}")
            return False
        except Exception as e:
            print(f"   💥 Download failed: {e}")
            return False
    
    def download_video_image_pair(self, video_name, image_name, output_dir):
        """
        Download a video-image pair from Azure storage
        
        Args:
            video_name (str): Video filename (e.g., "000005000016.0_processed.mp4")
            image_name (str): Image filename (e.g., "image_00001315.jpg")
            output_dir (str): Local directory to save files
            
        Returns:
            tuple: (video_success, image_success, video_path, image_path)
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Construct blob paths
        video_blob_path = f"{self.raw_video_path}/{video_name}"
        image_blob_path = f"{self.celeba_path}/{image_name}"
        
        # Construct local paths
        video_local_path = output_path / video_name
        image_local_path = output_path / image_name
        
        print(f"🎯 Downloading pair: {video_name} + {image_name}")
        
        # Download video
        video_success = self.download_file(video_blob_path, str(video_local_path))
        
        # Download image
        image_success = self.download_file(image_blob_path, str(image_local_path))
        
        return video_success, image_success, str(video_local_path), str(image_local_path)
    
    def download_first_pair_from_batch(self, batch_mapping_file, output_dir):
        """
        Download the first video-image pair from a batch mapping file
        
        Args:
            batch_mapping_file (str): Path to batch mapping JSON file
            output_dir (str): Directory to save downloaded files
            
        Returns:
            dict: Download results and metadata
        """
        print(f"📖 Loading batch mapping: {batch_mapping_file}")
        
        with open(batch_mapping_file, 'r') as f:
            batch_data = json.load(f)
        
        mapping = batch_data['mapping']
        if not mapping:
            raise ValueError("No mappings found in batch file")
        
        # Get first pair
        first_video = list(mapping.keys())[0]
        first_image = mapping[first_video]
        
        print(f"📍 First pair: {first_video} -> {first_image}")
        
        # Download the pair
        video_success, image_success, video_path, image_path = self.download_video_image_pair(
            first_video, first_image, output_dir
        )
        
        # Create download report
        report = {
            "download_info": {
                "batch_file": batch_mapping_file,
                "batch_number": batch_data.get('batch_info', {}).get('batch_number'),
                "downloaded_at": datetime.now().isoformat(),
                "output_directory": output_dir
            },
            "pair_info": {
                "video_name": first_video,
                "image_name": first_image,
                "video_path": video_path,
                "image_path": image_path
            },
            "download_results": {
                "video_success": video_success,
                "image_success": image_success,
                "both_successful": video_success and image_success
            },
            "azure_paths": {
                "video_blob": f"{self.raw_video_path}/{first_video}",
                "image_blob": f"{self.celeba_path}/{first_image}",
                "container": self.container_name
            }
        }
        
        return report

def main():
    """Main function to download first pair from batch_001"""
    print("🚀 Azure Batch Downloader")
    print("=" * 50)
    
    # Paths
    batch_file = "/root/autodl-tmp/VLM_forgery_detection/data/batch/batch_001/video_image_mapping.json"
    output_dir = "/root/autodl-tmp/VLM_forgery_detection/data/batch_data"
    
    try:
        # Initialize downloader
        downloader = AzureBatchDownloader()
        
        # Download first pair from batch_001
        report = downloader.download_first_pair_from_batch(batch_file, output_dir)
        
        # Save download report
        report_file = Path(output_dir) / "download_report.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print(f"\n📊 Download Summary:")
        print("=" * 30)
        print(f"Video: {'✅ SUCCESS' if report['download_results']['video_success'] else '❌ FAILED'}")
        print(f"Image: {'✅ SUCCESS' if report['download_results']['image_success'] else '❌ FAILED'}")
        print(f"Overall: {'✅ COMPLETE' if report['download_results']['both_successful'] else '⚠️ PARTIAL'}")
        
        if report['download_results']['both_successful']:
            print(f"\n📁 Files saved to: {output_dir}")
            print(f"   🎬 Video: {report['pair_info']['video_name']}")
            print(f"   🖼️ Image: {report['pair_info']['image_name']}")
        
        print(f"\n📋 Report saved: {report_file}")
        
    except FileNotFoundError as e:
        print(f"❌ Batch file not found: {e}")
        sys.exit(1)
        
    except Exception as e:
        print(f"💥 Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()