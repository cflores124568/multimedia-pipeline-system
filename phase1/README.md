# Phase 1: Frame Range Processor

Processes Baselight export files and Xytech work orders to generate CSV reports for post-production workflows. Handles path translation between local storage and facility networks.

## What it does

Takes two input files:
- **Baselight export**: Contains file paths and frame numbers that need fixing
- **Xytech work order**: Contains job info and file locations

Outputs a CSV file with consolidated frame ranges and proper facility paths.

## Key features

- Automatically finds files in common locations (current dir, Downloads, Desktop, Documents)
- Converts local Baselight paths to facility storage paths  
- Groups consecutive frames into ranges (1,2,3,4,5 becomes 1-5)
- Handles bad data like `<null>` and `<err>` values
- Flexible command line options

## Usage

### Basic usage
```bash
python frame_range_processor.py -x xytech_workorder.txt -b baselight_export.txt
```

### With custom output
```bash
python frame_range_processor.py -x xytech_data.txt -b baselight_data.txt -o my_report.csv
```

### Full paths
```bash
python frame_range_processor.py \
    --xytech /path/to/workorders/job_12345.txt \
    --baselight /path/to/exports/dailies_export.txt \
    --output /facility/reports/frame_fixes_12345.csv
```

### Auto file discovery
```bash
# Script will search for files automatically
python frame_range_processor.py -x "Xytech Workorder 2024.txt" -b "Baselight Export.txt"
```

## Arguments

- `-x, --xytech` - Xytech work order file (required)
- `-b, --baselight` - Baselight export file (required)  
- `-o, --output` - Output CSV file (default: frame-fixes.csv)

## Sample input/output

**Baselight export:**
```
/mnt/baselightfilesystem1/production/project_a/shot001.dpx 1001 1002 1003
/mnt/baselightfilesystem1/production/project_a/shot002.dpx 2001 2002 <null> 2004
```

**Xytech work order:**
```
Producer: Jane Smith  
Operator: John Doe
Job: Feature Film Color Correction
Location:
/facility/storage/production/project_a/shot001.dpx
/facility/storage/production/project_a/shot002.dpx
Notes: Priority delivery for client review
```

**CSV output:**
```csv
Producer: Jane Smith,Operator: John Doe,Job: Feature Film Color Correction,Notes: Priority delivery for client review
,,,
,,,
Location:,Frames to Fix:,
/facility/storage/production/project_a/shot001.dpx,1001-1003
/facility/storage/production/project_a/shot002.dpx,"2001-2002,2004"
```

## How path translation works

Baselight uses local storage paths like `/mnt/baselightfilesystem1/production/...` but the facility needs network paths. The script matches them by looking for keywords like "production" and strips the local mount point.

## Error handling

- Skips invalid frame numbers (`<null>`, `<err>`, etc.)
- Warns when files aren't found
- Handles duplicate frame numbers
- Searches multiple directories if file isn't found

Built for post-production environments where colorists work locally but final delivery needs facility paths.
