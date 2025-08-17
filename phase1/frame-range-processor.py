import csv
import argparse
import os
import sys

def find_file(filename):
    #Checkif filename is a full or relative path
    if os.path.isfile(filename):
        return filename
    #Search common directories
    search_paths = [
        '.',  #current directory
        os.path.expanduser('~/Downloads'),  
        os.path.expanduser('~/Desktop'),    
        os.path.expanduser('~/Documents'),  
    ]
    for path in search_paths:
        full_path = os.path.join(path, filename)
        if os.path.isfile(full_path):
            return full_path
    return None  

#Parse command line arguments
parser = argparse.ArgumentParser(description='Process Xytech and Baselight files to create CSV output')
parser.add_argument('-x', '--xytech', required=True, help='Path to the Xytech workorder file')
parser.add_argument('-b', '--baselight', required=True, help='Path to the Baselight export file')
parser.add_argument('-o', '--output', default='frame-fixes.csv', help='Output CSV file path')
args = parser.parse_args()
#Find and validate input files 
xytech_file = find_file(args.xytech)
if not xytech_file:
    print(f"Error: Xytech file '{args.xytech}' not found in current directory, Downloads, Desktop, or Documents")
    sys.exit(1)

baselight_file = find_file(args.baselight)
if not baselight_file:
    print(f"Error: Baselight file '{args.baselight}' not found in current directory, Downloads, Desktop, or Documents")
    sys.exit(1)

output_file = args.output
print(f"Found Xytech file: {xytech_file}")
print(f"Found Baselight file: {baselight_file}")

PATH_KEYWORDS = ['production', 'baselightfilesystem1']

def clean_up_path(file_path):
    #Look for keywords and remove everything before them
    for keyword in PATH_KEYWORDS:
        if keyword in file_path:
            parts = file_path.split('/')
            for i in range(len(parts)):
                if parts[i] == keyword:
                    return '/'.join(parts[i+1:])  
    return file_path  

def make_frame_ranges(frame_numbers):
    #Convert a list of frame numbers into ranges 1,2,3 -> 1-3
    if not frame_numbers:
        return []
    
    frame_numbers = sorted(frame_numbers)
    ranges = []
    start_frame = frame_numbers[0]
    end_frame = frame_numbers[0]
    
    for frame in frame_numbers[1:]:
        if frame == end_frame + 1:
            end_frame = frame #Continue the range
        else:
            if start_frame == end_frame: #End current range and start a new one
                ranges.append(str(start_frame))
            else:
                ranges.append(f"{start_frame}-{end_frame}")
            start_frame = frame
            end_frame = frame
    
    #Handle the final range or single frame
    if start_frame == end_frame:
        ranges.append(str(start_frame))
    else:
        ranges.append(f"{start_frame}-{end_frame}")
    
    return ranges

def is_valid_frame_data(data):
    #Reject non-number data 
    if not data or not isinstance(data, str):
        return False
    data_str = data.strip().lower()  
    invalid_values = ['<null>', '<err>', 'null', 'err', '']
    if data_str in invalid_values:
        return False
    return data_str.isdigit()  #Ensure it's a valid frame number

#Parse Baselight file to map locations to their frame numbers
location_frames = {}

with open(baselight_file, 'r') as file:
    for line in file:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split()
        if not parts:
            continue
            
        location = clean_up_path(parts[0])  #First part is the file path
        
        if location not in location_frames:
            location_frames[location] = []
        
        for part in parts[1:]:  #Remaining parts are frame numbers
            if not is_valid_frame_data(part):
                print(f"Warning: Skipping invalid frame data '{part}' for location {location}")
                continue
            try:
                frame_number = int(part)  
                location_frames[location].append(frame_number)
            except ValueError:
                print(f"Warning: Could not parse frame number '{part}' for location {location}")
                continue

#Build frame ranges for each location
location_ranges = {}
for location in location_frames:
    #Use set to remove duplicate frames if any 
    unique_frames = list(set(location_frames[location]))
    location_ranges[location] = make_frame_ranges(unique_frames)

#Parse Xytech file for header info and locations
with open(xytech_file, 'r') as file:
    all_lines = [line.strip() for line in file if line.strip()]

#Extract producer, operator, and job from lines 2-4
producer = all_lines[1].split(":", 1)[1].strip()
operator = all_lines[2].split(":", 1)[1].strip()
job = all_lines[3].split(":", 1)[1].strip()
location_start = all_lines.index("Location:") + 1
notes_start = all_lines.index("Notes:") + 1
notes = all_lines[notes_start]
location_list = all_lines[location_start:notes_start-1]
#Build CSV entries by matching Xytech locations to Baselight frame ranges
output_entries = []

for original_location in location_list:
    cleaned_location = clean_up_path(original_location)  #Clean for matching with Baselight paths
    
    if cleaned_location in location_ranges and location_ranges[cleaned_location]:
        frame_ranges = location_ranges[cleaned_location]
        for frame_range in frame_ranges:
            output_entries.append([original_location, frame_range])  #keep original path for output
    else:
        print(f"Warning: No frame data found for location: {original_location}")
        output_entries.append([original_location, "No frames to fix"])  

#Write location/frame data to CSV
try:
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([f"Producer: {producer}", f"Operator: {operator}", f"Job: {job}", f"Notes: {notes}"])
        writer.writerow([])
        writer.writerow([])
        writer.writerow(["Location:", "Frames to Fix:"]) 
        for entry in output_entries:
            writer.writerow(entry)  
            
    print(f"Success! Output file created: {output_file}")
    
except Exception as e:
    print(f"Error writing output file: {e}")
    exit(1)
