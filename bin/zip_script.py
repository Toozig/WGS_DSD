import argparse
import os
import datetime
import zipfile
import yaml
import shlex

def command_to_yaml(command, output_file):
    # Split the command into parts
    parts = shlex.split(command)

    # Create a dictionary to hold the variables and their values
    data = {}

    # Iterate over the parts of the command
    for i in range(len(parts)):
        # If the part starts with '--', it's a flag
        if parts[i].startswith('--'):
            # The next part is the value for this flag
            if i + 1 < len(parts) and not parts[i + 1].startswith('--'):
                data[parts[i].lstrip('--')] = parts[i + 1]

    # Add the full command as a variable
    data['full_command'] = command

    # Write the data to a YAML file
    with open(output_file, 'w') as f:
        yaml.dump(data, f)

    return output_file

def zip_files_in_dir(output_file,dir_path, output_dir, command, max_files=100, max_size=5*1024*1024):
    # Check if the directory is a git repository
    is_git = '.git' in os.listdir(dir_path)

    # Get all files in the directory
    all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

    # Filter files based on the conditions
    if is_git:
        files_to_zip = all_files
    else:
        files_to_zip = [f for f in all_files if os.path.getsize(os.path.join(dir_path, f)) < max_size][:max_files]

    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Get current date
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')

    # Create output directory name
    output_dir_name = os.path.join(output_dir, os.path.basename(dir_path) + '_' + current_date + '.zip')

    # Create a ZipFile object
    with zipfile.ZipFile(output_dir_name, 'w') as zipf:
        # Add files to the zip file
        for f in files_to_zip:
            zipf.write(os.path.join(dir_path, f), arcname=f)

        # add the output_file
        file_name = os.path.basename(output_file)
        zipf.write(output_file, arcname=file_name)

        # Add the YAML file to the zip file
        yaml_file = command_to_yaml(command, os.path.join(output_dir, '/tmp/toozig/command.yml'))
        zipf.write(yaml_file, arcname='command.yml')

    # Return the path to the zip file
    return output_dir_name

def main():
    parser = argparse.ArgumentParser(description='Gzip files in a directory.')
    parser.add_argument('--dir_path', required=True, help='Path to the directory.')
    parser.add_argument('--output_dir', required=True, help='Path to the output directory.')
    parser.add_argument('--max_files', type=int, default=100, help='Maximum number of files to gzip.')
    parser.add_argument('--max_size', type=int, default=5*1024*1024, help='Maximum size of files to gzip in bytes.')
    parser.add_argument('--command', required=True, help='The command to convert to YAML and add to the zip file.')

    args = parser.parse_args()

    # Check if the directory exists
    if not os.path.isdir(args.dir_path):
        print(f"Error: Directory '{args.dir_path}' does not exist.")
        return

    # Gzip the files
    output_dir = zip_files_in_dir(args.dir_path, args.output_dir, args.command, args.max_files, args.max_size)

    print(f"Gzipped files are in: {output_dir}")

if __name__ == "__main__":
    main()