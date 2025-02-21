import yaml

def load_yaml_files(file_paths):
    merged_config = {}
    for path in file_paths:
        with open(path, "r") as file:
            config = yaml.safe_load(file)
            if config:
                merged_config.update(config)
    return merged_config