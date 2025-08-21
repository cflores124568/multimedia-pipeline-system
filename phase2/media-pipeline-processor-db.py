import argparse
import sys
import os
import csv
from datetime import datetime
from pymongo import MongoClient
import getpass

def find_file(filename):
    #Check iffilename is full or relative path
    if os.path.isfile(filename):
        return filename
    #Search common directories if file isn't in current location
    search_paths = [
        '.',  #Current directory
        './input',  #Structured github folders
        './input/baselight',  
        './input/flame', 
        './input/xytech',  
        os.path.expanduser('~/Downloads'),  
        os.path.expanduser('~/Desktop'),  
        os.path.expanduser('~/Documents'),  
    ]
    for path in search_paths:
        full_path = os.path.join(path, filename)
        if os.path.isfile(full_path):
            return full_path
    return None

def is_valid_frame_data(data):
    #Reject invalid frame data like <null> or <err>
    if not data or not isinstance(data, str):
        return False
    data_str = data.strip().lower()
    invalid_values = ['<null>', '<err>', 'null', 'err', '']
    if data_str in invalid_values:
        return False
    return data_str.isdigit()

def parse_arguments():
    parser = argparse.ArgumentParser(description='Process Baselight/Flame and Xytech files for CSV or MongoDB output')
    parser.add_argument("--files", nargs='+', dest="workFiles", help="List of Baselight/Flame files to process")
    parser.add_argument("--x", "--xytech", help="Path to the Xytech workorder file")
    parser.add_argument("--verbose", action="store_true", help="Print detailed processing information")
    parser.add_argument("--o", "--output", choices=['CSV', 'DB'], help="Choose output format: CSV or MongoDB")
    parser.add_argument("--view", action="store_true", help="Display contents of the database")
    parser.add_argument("--date", help="Export CSV for a specific date (format: YYYYMMDD)")
    parser.add_argument("--clear", action="store_true", help="Delete all database records (use with caution)")
    return parser.parse_args()

def parse_db_files(args):
    #Data structures for parsed results
    parsed_data = []
    xytech_data = None
    
    print("Processing input files...")
    if args.xytech:
        #Locate and validate Xytech file
        xytech_file = find_file(args.xytech)
        if not xytech_file:
            print(f"Error: Xytech file '{args.xytech}' not found")
            sys.exit(1)
        #Parse Xytech file for metadata and locations
        xytech_data = parse_xytech_file(xytech_file)
        if args.verbose:
            print(f"Found Xytech file: {xytech_file}")
            print(f"Parsed Xytech file: {xytech_data['job']} by {xytech_data['operator']}")
    
    #Loop through each input file (Baselight or Flame)
    for file_path in args.workFiles:
        found_file = find_file(file_path)
        if not found_file:
            print(f"Error: File '{file_path}' not found")
            continue
            
        if args.verbose:
            print(f"Found file: {found_file}")
            print(f"Processing file: {found_file}")
            
        #Extract user and date from filename
        filename_info = parse_filename(found_file)
        
        try:
            with open(found_file, 'r') as file:
                content = file.read().strip()
                #Determine file type and parse accordingly
                if is_baselight_file(content):
                    file_data = parse_baselight_content(content, filename_info, xytech_data, args)
                elif is_flame_file(content):
                    file_data = parse_flame_content(content, filename_info, xytech_data, args)
                else:
                    print(f"Warning: Unknown file format for {found_file}")
                    continue
                
                #Add parsed data to results
                parsed_data.extend(file_data)
                
        except FileNotFoundError:
            print(f"Error: File {found_file} not found")
        except Exception as e:
            print(f"Error processing {found_file}: {e}")
    
    print(f"Processed {len(args.workFiles)} files, parsed {len(parsed_data)} entries")
    return parsed_data, xytech_data

def parse_xytech_file(xytech_path):
    #Read and parse Xytech file for metadata and locations
    with open(xytech_path, 'r') as file:
        all_lines = [line.strip() for line in file if line.strip()]
    
    #Extract producer, operator, and job from lines 2-4
    producer = all_lines[1].split(":", 1)[1].strip() if len(all_lines) > 1 else ""
    operator = all_lines[2].split(":", 1)[1].strip() if len(all_lines) > 2 else ""
    job = all_lines[3].split(":", 1)[1].strip() if len(all_lines) > 3 else ""
    
    #Find where locations and notes sections start
    location_start = all_lines.index("Location:") + 1 if "Location:" in all_lines else len(all_lines)
    notes_start = all_lines.index("Notes:") + 1 if "Notes:" in all_lines else len(all_lines)
    notes = all_lines[notes_start] if notes_start < len(all_lines) else ""
    location_list = all_lines[location_start:notes_start-1] if location_start < notes_start else []
    
    #Return structured Xytech data
    return {
        'producer': producer,
        'operator': operator,
        'job': job,
        'locations': location_list,
        'notes': notes
    }

def parse_filename(file_path):
    #Extract user and date from filename (expected format: user_date)
    basename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(basename)[0]
    
    parts = name_without_ext.split('_')
    if len(parts) < 2:
        raise ValueError(f"Filename {file_path} doesn't match expected format (user_date)")
    user, date_str = parts[-2:]
    
    #Validate date format
    try:
        date_obj = datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        raise ValueError(f"Invalid date format in filename: {date_str}")
    
    return {
        'user': user,
        'date': date_obj,
        'date_string': date_str,
        'filename': basename
    }

def is_baselight_file(content):
    #Check if content matches Baselight format: paths starting with / followed by frame numbers
    lines = content.split('\n')
    for line in lines:
        line = line.strip()
        if line:
            parts = line.split()
            if len(parts) >= 2 and parts[0].startswith('/') and not parts[0].startswith('/net/flame-archive'):
                try:
                    for part in parts[1:]:
                        int(part)
                    return True
                except ValueError:
                    continue
    return False

def is_flame_file(content):
    #Check if content matches Flame format: paths starting with /net/flame-archive
    lines = content.split('\n')
    for line in lines:
        line = line.strip()
        if line and line.startswith('/net/flame-archive'):
            return True
    return False

#Words to look for when matching paths; helps align Baselight/Flame locations with Xytech folders
PATH_KEYWORDS = ['Avatar']

def get_logical_path(file_path):
    #Extract logical path starting from specific keywords for matching
    for keyword in PATH_KEYWORDS:
        if keyword in file_path:
            keyword_index = file_path.find(keyword)
            return file_path[keyword_index:]
    return file_path

def find_matching_xytech_location(baselight_flame_location, xytech_locations, args):
    #Match Baselight/Flame locations to Xytech locations
    if not xytech_locations:
        return baselight_flame_location
    
    bf_logical = get_logical_path(baselight_flame_location)
    
    if args.verbose:
        print(f"Looking for Xytech match for: '{bf_logical}'")
    
    #Compare logical paths
    for xytech_location in xytech_locations:
        xytech_logical = get_logical_path(xytech_location.strip())
        if bf_logical.lower() == xytech_logical.lower():
            if args.verbose:
                print(f"  ✓ Found match: '{xytech_location.strip()}'")
            return xytech_location.strip()
    
    if args.verbose:
        print(f"  ✗ No match found, keeping original")
    
    return baselight_flame_location

def parse_baselight_content(content, filename_info, xytech_data, args):
    #Parse Baselight file to map locations to frame numbers
    data = []
    lines = content.split('\n')
    location_frames = {}
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        parts = line.split()
        if len(parts) >= 2:
            original_path = parts[0]
            if original_path not in location_frames:
                location_frames[original_path] = []
            
            #Collect valid frame numbers
            for part in parts[1:]:
                if not is_valid_frame_data(part):
                    if args.verbose:
                        print(f"Warning: Skipping invalid frame data '{part}' for location {original_path}")
                    continue
                try:
                    frame_number = int(part)
                    location_frames[original_path].append(frame_number)
                except ValueError:
                    if args.verbose:
                        print(f"Warning: Could not parse frame number '{part}' for location {original_path}")
                    continue
    
    #Build data entries with frame ranges
    for original_path in location_frames:
        unique_frames = list(set(location_frames[original_path]))  #Remove duplicates
        frame_ranges = make_frame_ranges(unique_frames)
        final_location = find_matching_xytech_location(original_path, xytech_data['locations'], args) if xytech_data and xytech_data['locations'] else original_path
        
        #Create structured entry for each location
        entry = {
            'file_type': 'Baselight',
            'user': filename_info['user'],
            'date': filename_info['date'],
            'filename': filename_info['filename'],
            'location': final_location,
            'frames': frame_ranges,
            'storage': original_path,
            'producer': xytech_data['producer'] if xytech_data else '',
            'operator': xytech_data['operator'] if xytech_data else '',
            'job': xytech_data['job'] if xytech_data else '',
            'notes': xytech_data['notes'] if xytech_data else ''
        }
        data.append(entry)
    
    return data

def parse_flame_content(content, filename_info, xytech_data, args):
    #Parse Flame file to map locations to frame numbers
    data = []
    lines = content.split('\n')
    location_frames = {}
    
    for line in lines:
        line = line.strip()
        if line:
            parts = line.split()
            if len(parts) >= 2 and parts[0].startswith('/net/flame-archive'):
                #Find where frame numbers start in the line
                frame_start_index = 1
                for i in range(1, len(parts)):
                    try:
                        int(parts[i])
                        frame_start_idx = i
                        break
                    except ValueError:
                        continue
                
                location_parts = parts[1:frame_start_index]
                flame_location = ' '.join(location_parts)
                if flame_location not in location_frames:
                    location_frames[flame_location] = []
                
                #Collect valid frame numbers
                for frame in parts[frame_start_index:]:
                    if not is_valid_frame_data(frame):
                        if args.verbose:
                            print(f"Warning: Skipping invalid frame data '{frame}' for location {flame_location}")
                        continue
                    try:
                        frame_number = int(frame)
                        location_frames[flame_location].append(frame_number)
                    except ValueError:
                        if args.verbose:
                            print(f"Warning: Could not parse frame number '{frame}' for location {flame_location}")
                        continue
    
    #Build data entries with frame ranges
    for flame_location in location_frames:
        unique_frames = list(set(location_frames[flame_location]))  #Remove duplicates
        frame_ranges = make_frame_ranges(unique_frames)
        flame_path = flame_location.replace(' ', '/')
        if not flame_path.startswith('/'):
            flame_path = '/' + flame_path

        #Determine final location to use
        if xytech_data and xytech_data['locations']:
            final_location = find_matching_xytech_location(flame_path, xytech_data['locations'], args)
        else:
            final_location = flame_path
        
        #Create structured entry for each location
        entry = {
            'file_type': 'Flame',
            'user': filename_info['user'],
            'date': filename_info['date'],
            'filename': filename_info['filename'],
            'storage': '/net/flame-archive',
            'location': final_location,
            'frames': frame_ranges,
            'producer': xytech_data['producer'] if xytech_data else '',
            'operator': xytech_data['operator'] if xytech_data else '',
            'job': xytech_data['job'] if xytech_data else '',
            'notes': xytech_data['notes'] if xytech_data else ''
        }
        data.append(entry)
    
    return data

def clean_up_path(file_path):
    #Trim path prefix before specific keywords for consistency
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
    
    #Build ranges by checking for consecutive frames
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

def sort_frame_ranges_numeric(frame_ranges):
    #Sort frame ranges numerically instead of alphabetically
    def get_start_frame(frame_range):
        #Get starting frame number
        if '-' in frame_range:
            return int(frame_range.split('-')[0])
        else:
            return int(frame_range)
    return sorted(frame_ranges, key=get_start_frame)

def output_to_csv(parsed_data, xytech_data):
    #Generate CSV output with locations and frame ranges
    if not parsed_data:
        print("No data to write to CSV")
        return
    
    #Create output directory if it doesn't exist
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    #Determine output filename based on date
    if parsed_data:
        date_str = parsed_data[0]['date'].strftime('%Y%m%d')
        output_file = os.path.join(output_dir, f'frame-fixes-{date_str}.csv')
    else:
        output_file = os.path.join(output_dir, 'phase2_frame-fixes.csv')
    
    #Extract metadata from Xytech data
    producer = xytech_data['producer'] if xytech_data else ''
    operator = xytech_data['operator'] if xytech_data else ''
    job = xytech_data['job'] if xytech_data else ''
    notes = xytech_data['notes'] if xytech_data else ''
    
    output_entries = []
    location_groups = {}
    #Group frames by location
    for entry in parsed_data:
        location = entry['location']
        if location not in location_groups:
            location_groups[location] = []
        location_groups[location].extend(entry['frames'])
    
    #Use Xytech location order to keep original sequence in output
    if xytech_data and xytech_data['locations']:
        #Process locations in the exact order they appear in Xytech file
        for xytech_location in xytech_data['locations']:
            if xytech_location in location_groups:
                if location_groups[xytech_location]:
                    #Sort frame ranges for ordered output
                    frame_ranges = sort_frame_ranges_numeric(location_groups[xytech_location])
                    for frame_range in frame_ranges:
                        output_entries.append([xytech_location, frame_range])
                else:
                    print(f"Warning: No frame data found for location: {xytech_location}")
                    output_entries.append([xytech_location, "No frames to fix"])
        
        #Handle any locations not in the Xytech file
        for location in location_groups:
            if location not in xytech_data['locations']:
                print(f"Warning: Location {location} not found in Xytech file, adding at end of file")
                if location_groups[location]:
                    frame_ranges = sort_frame_ranges_numeric(location_groups[location])
                    for frame_range in frame_ranges:
                        output_entries.append([location, frame_range])
                else:
                    output_entries.append([location, "No frames to fix"])
    else:
        #Fallback to processing all locations if no Xytech data
        for location in location_groups:
            if location_groups[location]:
                frame_ranges = sort_frame_ranges_numeric(location_groups[location])
                for frame_range in frame_ranges:
                    output_entries.append([location, frame_range])
            else:
                print(f"Warning: No frame data found for location: {location}")
                output_entries.append([location, "No frames to fix"])
    
    #Write data to CSV file with header
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
        sys.exit(1)

def insert_to_mongodb(parsed_data, args):
    #Insert parsed data into MongoDB database
    print("Inserting data into MongoDB...")
    try:
        #Connect to local MongoDB instance
        client = MongoClient('mongodb://localhost:27017/')
        db = client['MediaPipelineDB']
        scripts_collection = db['script_runs']
        files_collection = db['file_data']
        
        current_user = getpass.getuser()
        submitted_date = datetime.now()
        
        #Insert unique script run records
        unique_files = {}
        for entry in parsed_data:
            key = (entry['user'], entry['date'])
            unique_files[key] = entry
            
        for key, entry in unique_files.items():
            script_record = {
                'script_user': current_user,
                'file_user': entry['user'],
                'file_date': entry['date'],
                'submitted_date': submitted_date
            }
            result1 = scripts_collection.insert_one(script_record)
            if args.verbose:
                print(f"Inserted script run record: {result1.inserted_id}")
        
        #Insert individual file data records
        inserted_count = 0
        for entry in parsed_data:
            for frame_range in entry['frames']:
                file_record = {
                    'file_user': entry['user'],
                    'file_date': entry['date'],
                    'location': entry['location'],
                    'frames': [frame_range],
                    'storage': entry['storage'],
                    'filename': entry['filename'],
                    'file_type': entry['file_type'],
                    'producer': entry.get('producer', ''),
                    'operator': entry.get('operator', ''),
                    'job': entry.get('job', ''),
                    'notes': entry.get('notes', '')
                }
                result2 = files_collection.insert_one(file_record)
                inserted_count += 1
                if args.verbose:
                    print(f"Inserted file data record: {result2.inserted_id}")
        
        print(f"Successfully inserted {inserted_count} records into MongoDB")
        client.close()
        
    except Exception as e:
        print(f"Error inserting to MongoDB: {e}")
        sys.exit(1)

def view_database():
    print("Viewing database contents...")
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['MediaPipelineDB']
        scripts_collection = db['script_runs']
        files_collection = db['file_data']
        
        print("=== DATABASE CONTENTS ===\n")
        
        #Show script run records
        print("SCRIPT RUNS COLLECTION:")
        print("-" * 50)
        scripts = scripts_collection.find()
        script_count = 0
        for i, script_run in enumerate(scripts, 1):
            script_count += 1
            print(f"{i}. Script User: {script_run.get('script_user')}")
            print(f"   File User: {script_run.get('file_user')}")
            print(f"   File Date: {script_run.get('file_date')}")
            print(f"   Submitted: {script_run.get('submitted_date')}")
            print()
        
        print(f"Total script runs: {script_count}\n")
        
        #Show file data records
        print("FILE DATA COLLECTION:")
        print("-" * 50)
        files = files_collection.find()
        file_count = 0
        for i, file_data in enumerate(files, 1):
            file_count += 1
            print(f"{i}. User: {file_data.get('file_user')}")
            print(f"   Date: {file_data.get('file_date')}")
            print(f"   Location: {file_data.get('location')}")
            print(f"   Frames: {file_data.get('frames')}")
            print(f"   Storage: {file_data.get('storage')}")
            print(f"   File Type: {file_data.get('file_type')}")
            print()
        
        print(f"Total file records: {file_count}")
        client.close()
        
    except Exception as e:
        print(f"Error viewing database: {e}")
        sys.exit(1)

def export_csv_by_date(date_str, args):
    #Export MongoDB data for a specific date to CSV
    print(f"Exporting CSV for date: {date_str}")
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['MediaPipelineDB']
        files_collection = db['file_data']
        #Validate date format
        try:
            target_date = datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            print(f"Error: Invalid date format {date_str}. Expected YYYYMMDD")
            sys.exit(1)
        
        #Query records for the specified date
        cursor = files_collection.find({'file_date': target_date})
        db_data = list(cursor)
        if not db_data:
            print(f"No data found for date: {date_str}")
            client.close()
            return
        
        if args.verbose:
            print(f"Found {len(db_data)} records for date: {date_str}")
        
        #Create output directory and file
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"frame-fixes-{date_str}.csv")
        
        #Group data by location
        location_groups = {}
        producer = operator = job = notes = ""
        for entry in db_data:
            location = entry['location']
            if location not in location_groups:
                location_groups[location] = []
            location_groups[location].extend(entry['frames'])
            #Extract metadata if not already set
            if not producer and entry.get('producer'):
                producer = entry['producer']
            if not operator and entry.get('operator'):
                operator = entry['operator']
            if not job and entry.get('job'):
                job = entry['job']
            if not notes and entry.get('notes'):
                notes = entry['notes']
        #Try to get original Xytech file to keep original order of folder locations
        xytech_file_path = f"Xytech_{date_str}.txt"
        xytech_locations = []
        #Find and parse the respective Xytech file
        xytech_file = find_file(xytech_file_path)
        if xytech_file:
            try:
                xytech_data = parse_xytech_file(xytech_file)
                xytech_locations = xytech_data['locations']
                print(f"Found Xytech file: {xytech_file}")
            except Exception as e:
                print(f"Could not parse Xytech file: {e}")
                xytech_locations = []
        else:
            print(f"Xytech file {xytech_file_path} not found")
        output_entries = []
        #use xytech location order if available else alphabetical?
        if xytech_locations:
            for xytech_location in xytech_locations:
                if xytech_location in location_groups:
                    if location_groups[xytech_location]:
                        frame_ranges = sort_frame_ranges_numeric(location_groups[xytech_location])
                        for frame_range in frame_ranges:
                            output_entries.append([xytech_location, frame_range])
                    else:
                        output_entries.append([xytech_location, "No frames to fix"])
            #Add any locations not in Xytech file
            for location in location_groups:
                if location not in xytech_locations:
                    print(f"Warning: Location {location} not in Xytech file, adding to end of file")
                    if location_groups[location]:
                        frame_ranges = sort_frame_ranges_numeric(location_groups[location])
                        for frame_range in frame_ranges:
                            output_entries.append([location, frame_range])
                    else:
                        output_entries.append([location, "No frames to fix"])
        else:
            for location in location_groups:
                if location_groups[location]:
                    frame_ranges = sort_frame_ranges_numeric(location_groups[location])
                    for frame_range in frame_ranges:
                        output_entries.append([location, frame_range])
                else:
                    output_entries.append([location, "No frames to fix"])
                    
        #Write data to CSV with header
        try:
            with open(output_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([f"Producer: {producer}", f"Operator: {operator}", f"Job: {job}", f"Notes: {notes}"])
                writer.writerow([])
                writer.writerow([])
                writer.writerow(["Location:", "Frames to Fix:"])
                for entry in output_entries:
                    writer.writerow(entry)
            print(f"Successfully created output file: {output_file}")
        except Exception as e:
            print(f"Error writing CSV file: {e}")
            sys.exit(1)
        client.close()
    except Exception as e:
        print(f"Error exporting CSV for date {date_str}: {e}")
        sys.exit(1)

def clear_database():
    print("Clearing the database...")
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['MediaPipelineDB']
        scripts_result = db['script_runs'].delete_many({})
        files_result = db['file_data'].delete_many({})
        print(f"Deleted {scripts_result.deleted_count} script run records")
        print(f"Deleted {files_result.deleted_count} file data records")
        print("Database cleared successfully")
        client.close()
    except Exception as e:
        print(f"Error clearing database: {e}")
        sys.exit(1)

def main():
    #Main execution flow: parse args, process files, and handle output
    args = parse_arguments()
    print("Starting script execution...")

    #Handle database clear operation
    if args.clear:
        clear_database()
        return
    #Handle CSV export for a specific date
    if args.date:
        export_csv_by_date(args.date, args)
        return
    #Check for valid input
    if not (args.workFiles or args.query or args.view):
        print("No Baselight/Flame files selected or options specified")
        sys.exit(2)

    if args.workFiles:
        print(f"Processing {len(args.workFiles)} work files with output: {args.output or 'none'}")
        parsed_data, xytech_data = parse_db_files(args)
        if args.verbose:
            print(f"Parsed {len(parsed_data)} entries")
            for entry in parsed_data:
                print(entry)
        #Output to CSV or MongoDB based on user choice
        if args.output == 'CSV':
            output_to_csv(parsed_data, xytech_data)
        elif args.output == 'DB':
            insert_to_mongodb(parsed_data, args)

    #View database contents if requested
    if args.view:
        view_database()

if __name__ == "__main__":
    main()
