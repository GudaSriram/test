import os
import yaml

def get_files_containing_text1(directory):
    dispatch_folders = {}
    oie_folders = {}
    record_folders = {}

    def collect_file_paths(directory):
        if not os.path.exists(directory):
            return  # Exit if directory does not exist

        items = os.listdir(directory)
        for item in items:
            item_path = os.path.join(directory, item)
            if os.path.isdir(item_path):
                collect_file_paths(item_path)
            else:
                if 'dispatchValues' in item:
                    if directory not in dispatch_folders:
                        dispatch_folders[directory] = (item_path, [])
                    dispatch_folders[directory][1].append(item_path)
                # elif 'oieValues' in item:
                #     if directory not in oie_folders:
                #         oie_folders[directory] = (item_path, [])
                #     oie_folders[directory][1].append(item_path)
                # if 'recordsValues' in item:
                #     if directory not in record_folders:
                #         record_folders[directory] = (item_path, [])
                #     record_folders[directory][1].append(item_path)

    collect_file_paths(directory)
    return dispatch_folders, oie_folders, record_folders

def count_mappings_in_file(filename):
    mapping_names = []

    def has_desired_keys(mapping):
        desired_keys = {
            'replicas': int,
            'enabled': bool,
            'cpu': int
        }
        return any(key in mapping and isinstance(mapping[key], desired_keys[key]) for key in desired_keys)

    def find_mappings(data, parent_key='', level=0):
        for key, value in data.items():
            if isinstance(value, dict):
                if has_desired_keys(value):
                    mapping_names.append((' ' * level * 2) + key)
                # Recursively search nested dictionaries
                find_mappings(value, key, level + 1)

    with open(filename, 'r') as file:
        data = yaml.safe_load(file)

    find_mappings(data)
    return len(mapping_names), mapping_names

def collect_mapping_data(folders, output_filename):
    for parent_folder, (first_instance, files) in folders.items():
        all_mapping_names = []
        for file in files:
            _, mapping_names = count_mappings_in_file(file)
            all_mapping_names.extend(mapping_names)

        output_file = os.path.join(parent_folder, output_filename)  # Define output file path

        with open(output_file, 'w') as file:
            file.write(f"Total number of mappings found: {len(all_mapping_names)}\n")
            file.write("Mapping names:\n")
            for name in all_mapping_names:
                file.write(f"  - {name}\n")
        print(f"Aggregated data saved to: {output_file}")

def main():
    source = r'C:\MyData\configurations'  # Replace with your directory
    try:
        dispatch_folders, oie_folders, record_folders = get_files_containing_text1(source)
        if not (dispatch_folders or oie_folders or record_folders):
            print("No 'dispatchValues', 'oieValues', or 'recordValues' files found.")
        else:
            if dispatch_folders:
                collect_mapping_data(dispatch_folders, "agg_dispatch.txt")
            if oie_folders:
                collect_mapping_data(oie_folders, "agg_oie.txt")
            if record_folders:
                collect_mapping_data(record_folders, "agg_record.txt")
    except FileNotFoundError:
        print(f"The folder '{source}' does not exist.")

if __name__ == "__main__":
    main()
