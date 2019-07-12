# network_builder

- Set up a virtual environment via Anaconda
  - `conda create --name network_builder`

- Geopandas is used throughout the code and is often challenging to install. It is often helpful to use only a single conda library source when working with geopandas. To exclude install to conda-forge:
  - `conda create -c conda-forge --override-channels -n geopandas geopandas`

- Install other libraries *before* geopandas
  - `conda install networkx`
  - `conda install pymssql`
  - `conda install geopandas`

- Update INRO path file from Emme to point to network_builder environment
    - Tools -> Application Options -> Modeller -> Python Path: `C:\anaconda\envs\network_builder`
    - Apply
    - Install Modeller Package
    
- activate network_builder environment and run the code after adjusting settings in **config.yaml** and **network_config.yaml**
  - `activate network_builder`
  - `python build_network.py`
