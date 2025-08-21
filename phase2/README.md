# Phase 2: Database Integration

Adds database storage to the frame range processor. Now you can store everything in MongoDB instead of just generating CSV files. It also handles Flame files

## What's new

**Database storage** - Everything gets saved to MongoDB so you can query it later instead of losing track of CSV files.

**Flame file support** - Processes Flame archive exports in addition to Baselight files. Automatically detects which format you're using.

**Multiple files at once** - Process a bunch of files in one go instead of running the script over and over.

**Better querying** - Pull up old data by date, view everything in the database, export historical stuff back to CSV.

## Setup

You'll need MongoDB running locally:

```bash
# Mac
brew install mongodb-community
brew services start mongodb-community

# Ubuntu  
sudo apt install mongodb
sudo systemctl start mongod
```

And pymongo:
```bash
pip install pymongo
```

## Usage

### Store stuff in the database
```bash
python media-pipeline_processor-db.py --files baselight_smith_20241201.txt flame_jones_20241201.txt --x Xytech_20241201.txt --o DB
```

### Look at what's stored
```bash
python media-pipeline_processor-db.py --view
```

### Export old data to CSV
```bash
python media-pipeline_processor-db.py --date 20241201
# Creates output/ folder if it doesn't exist, then saves the CSV there
```

### Clear everything. Be careful
```bash
python media-pipeline_processor-db.py --clear
```

### Still works like Phase 1
```bash
python media-pipeline_processor-db.py --files some_file.txt --x xytech.txt --o CSV
```

## File formats

**Baselight files** (same as Phase 1):
```
/mnt/baselightfilesystem1/production/Avatar/shot001.dpx 1001 1002 1003
```

**Flame files** (new):
```
/net/flame-archive Avatar shot001 1001 1002 1003
```

**Baselight and Flame filenames need to follow this pattern**: `username_YYYYMMDD.txt`
- `baselight_smith_20241201.txt` means user "smith" worked on Dec 1, 2024
- `flame_jones_20241215.txt` means user "jones" worked on Dec 15, 2024

## How the database works

Two collections get created:

**script_runs** - tracks who ran the script when
**file_data** - stores all the actual frame fix data

Each location/frame range combo gets its own record, so you can query by location, date, user, whatever.

## Command line options

| Flag | What it does |
|------|--------------|
| `--files file1.txt file2.txt` | Process these files |
| `--x xytech.txt` | Xytech work order file |
| `--o CSV` or `--o DB` | Output to CSV file or database |
| `--view` | Show everything in the database |
| `--date 20241201` | Export this date to CSV |
| `--clear` | Delete all database records |
| `--verbose` | Show detailed processing info |

## Example workflow

Process today's work:
```bash
python media-pipeline_processor-db.py --files baselight_smith_20241201.txt --x Xytech_20241201.txt --o DB
```

Later, export it:
```bash
python media-pipeline_processor-db.py --date 20241201
# Creates: output/frame-fixes-20241201.csv (makes the output/ folder if needed)
```

## What changed from Phase 1

- `--files` instead of `-b` (can handle multiple files now)
- Added `--o DB` option for database storage
- Added `--view`, `--date`, `--clear` for database operations
- Supports Flame files automatically
- Still generates the same CSV format when you need it

The script figures out if you're using Baselight or Flame files by looking at the paths. Baselight uses `/mnt/` stuff, Flame uses `/net/flame-archive`.
