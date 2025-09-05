# Azure Sync - VLM Forgery Detection Pipeline

**Complete Azure integration for batch video processing with download/upload capabilities**

## 📁 Directory Structure

```
/root/autodl-tmp/azure_sync/
├── batch_configs/              # Batch configuration JSONs (31 batches)
│   ├── batch_001/video_image_mapping.json    # 1000 video-image pairs
│   ├── batch_002/video_image_mapping.json
│   └── ...batch_031/
│
├── scripts/                    # Azure interaction scripts  
│   ├── azure_download.py       # ✅ Download videos/images from Azure
│   ├── azure_upload.py         # ✅ Upload processing results to Azure
│   └── azure_batch_downloader.py  # Original downloader with credentials
│
├── credentials/                # Azure authentication
│   ├── .env.template           # Template for credentials (safe to commit)
│   └── .env                    # Actual credentials (gitignored for security)
│
└── logs/                      # Operation logs
    ├── download.log
    └── upload.log
```

## 🔐 Azure Storage Configuration

- **Account**: `videoshots`
- **Container**: `videos`
- **Video Source**: `ruijian-research/raw/`
- **Image Source**: `ruijian-research/celeba-hq/`  
- **Results**: `ruijian-research/batch_results/batch_{ID}/`

## 📥 Download Usage

```bash
cd /root/autodl-tmp/azure_sync/scripts

# Download entire batch (1000 pairs)
python azure_download.py --batch 001 --output /root/downloads

# Download with limit
python azure_download.py --batch 002 --limit 50

# List available batches
python azure_download.py --list-batches
```

## 📤 Upload Usage

```bash
# Upload complete results only (recommended)
python azure_upload.py --batch 001 --results /path/to/batch_001_result --complete-only

# Check completeness before upload
python azure_upload.py --batch 001 --results /path/to/results --check-only
```

## 🚀 Production Workflow

```bash
# 1. Download → Process → Upload
python azure_download.py --batch 001 --limit 10
python individual_video_processor.py --batch 001 --mode production --limit 10  
python azure_upload.py --batch 001 --results /path/to/results --complete-only

# 2. Multi-VM deployment (each VM different batch)
# VM1: --batch 001, VM2: --batch 002, VM3: --batch 003
```

**31 batches × 1000 videos = 30,000 total videos ready for processing!** 🎯