# network_builder


- Set up a virtual environment via Anaconda prompt
  - `conda create --name network_builder`
  
 - Install the following libraries
  - `conda install geopandas`
  - `conda install networkx`
  - `conda install pymssql`
  - `pip install rasterstats`

- Update INRO path file from Emme to point to network_builder environment
    - Tools -> Application Options -> Modeller -> Python Path: `C:\anaconda\envs\network_builder`
    - Apply
    - Install Modeller Package
    
- activate network_builder environment and run the code after adjusting settings in **config.yaml** and **network_config.yaml**
  - `activate network_builder`
  - `python build_network.py`
