import os
import yaml

def get_files_containing_text1(directory):
    """
    Function recursively collects file paths containing "dispatchValues" file in the given directory.
    directory (str path): The directory to start from.
    Returns a dictionary where keys are parent directories containing dispatchValues files and values are tuples containing the first instance file name and a list of file paths.
    """
    dispatch_folders = {}

    def collect_file_paths(directory):
        if not os.path.exists(directory):
            raise FileNotFoundError(f"The folder '{directory}' does not exist.")

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
                    print(f"Found dispatchValues file at: {item_path}")

    collect_file_paths(directory) #recurring call
    return dispatch_folders

def count_mappings_in_file(filename):
    """
    Counts the number of mappings in a YAML file and returns their names as a list.
    Args:
        filename (str): The path to the YAML file.
    Returns:
        A tuple containing the count of mappings and a list of mapping names.
    """
    mapping_names = []

    def has_desired_keys(mapping):
        """
        Checks if the mapping has at least one of the desired keys with the correct type.
        Args:
            mapping (dict): The mapping to check.
        Returns:
            bool: True if the mapping contains any of the desired keys, False otherwise.
        """
        for key, value in mapping.items():
            if (key == 'replicas' and isinstance(value, int)) or \
               (key == 'enabled' and isinstance(value, bool)) or \
               (key == 'cpu' and isinstance(value, int)):
                return True
        return False

    def find_mappings(data, parent_key='', level=0):
        """
        Recursively finds mappings that contain the desired keys.
        Args:
            data (dict): The current level of the YAML data.
            parent_key (str): The key path to the current level.
            level (int): The current level of indentation.
        """
        for key, value in data.items():
            if isinstance(value, dict):
                if has_desired_keys(value):
                    mapping_names.append((' ' * level * 2) + key)
                # Recursively search nested dictionaries
                find_mappings(value, key, level + 1)

    if not os.path.exists(filename):
        raise FileNotFoundError(f"The file '{filename}' does not exist.")

    with open(filename, 'r') as file:
        data = yaml.safe_load(file)

    find_mappings(data)
    return len(mapping_names), mapping_names

def collect_mapping_data(dispatch_folders):
    """
    Collects mapping data from YAML files and writes it to separate text files in each parent folder.
    Args:
        dispatch_folders (dict): A dictionary where keys are parent directories containing dispatchValues files and values are tuples containing the first instance file name and a list of file paths.
    """
    for parent_folder, (first_instance, dispatch_files) in dispatch_folders.items():
        all_mapping_names = []
        for dispatch_file in dispatch_files:
            mapping_count, mapping_names = count_mappings_in_file(dispatch_file)
            all_mapping_names.extend(mapping_names)

        output_file = os.path.join(parent_folder, "aggregated.txt")  # Define output file path

        with open(output_file, 'w') as file:
            file.write(f"Total number of mappings found: {len(all_mapping_names)}\n")
            file.write("Mapping names:\n")
            for name in all_mapping_names:
                file.write(f"  - {name}\n")

def main():
    
    source = r'C:\MyData\configurations'
    dispatch_folders = get_files_containing_text1(source)
    collect_mapping_data(dispatch_folders)
    if len(dispatch_folders) < 1: print("No folders Found")

if __name__ == "__main__":
    main()
