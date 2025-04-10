import shutil
import os
from pathlib import Path
class FileSystem():
    """FileSystem class to handle file system operations."""

    def __init__(self, config):
        """Create directories based on the configuration."""
        self.config = config
        self.output_dir = self.create_directory(path_parts=[config.output_dir])
        self.emme_dir = self.create_directory(path = self.output_dir/config.emme_folder_name)
        self.log_dir = self.create_directory(path = self.output_dir/"logs")
        self.shapefile_dir = self.create_directory(path = self.output_dir/"shapefiles")
        self.build_file_dir = self.create_directory(path = self.output_dir/"build_files")

        self.roadway_dir = self.create_directory(path = self.build_file_dir/"roadway")
        self.transit_dir = self.create_directory(path = self.build_file_dir/"transit")
        self.turns_dir = self.create_directory(path = self.build_file_dir/"turns")
        self.shape_dir = self.create_directory(path = self.build_file_dir/"shape")
        self.extra_attributes_dir = self.create_directory(path = self.build_file_dir/"extra_attributes")
        

    def create_directory(self, path_parts: list=None, path: str=None) -> Path:
        """Create a directory if it doesn't exist."""
        if path_parts:
            path = Path(os.path.join(*path_parts))
        else:
            path_parts = path

        if not os.path.exists(path):
            os.makedirs(path)
            print(f"Directory {path} created.")
        else:
            print(f"Directory {path} already exists. Removing it.")
            shutil.rmtree(path)
            os.makedirs(path)
            print(f"Directory {path} created after removal.")   
        return path
    

       
        
     
    
