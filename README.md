# README #

## Setup

Install the package (you can use a virtual environment you created, or create a new one). The package was developed with Python 3.8, but should work with Python >= 3.6

To install with pip, simply run this from the project root:

```bash
pip install ./
```

If you intend to do changes in the package, you can install in editable mode:

```bash
pip install -e ./
```

# Example setup & code

Create a directory to store the data. By default, the example uses this structure:

```PROJECT_PATH```
   ├ local
   └ hdfs

Copy or rename the ```dev.conf.template``` to ```dev.conf```, and fill in the ```project_path```. For Windows,
you will might need to write it in this form ```E:\data/test_project```. If you use a different directory structure,
adjust the ```local_path``` / ```hdfs_path``` as needed

## Example code

You can run this from python shell or a notebook
```python
import sys
import os

from pathlib import Path

config_file = Path("config/dev.conf")

from pyhocon import ConfigFactory
config = ConfigFactory.parse_file(config_file)

from datamanager import DataManagerFactory
dm = DataManagerFactory.all(config)

from datamanager.types import DataContext

# Try a key that doesn't exist
dm.has("mykey") # False
# However, we do have data managers that can WRITE the key
dm.has("mykey", context=DataContext.WRITE) # True

# The result of this will be a pandas dataframe, because we have it
# configured in the pandas data manager
trx_pd = dm.load("raw_transactions")

# The 'transactions' dataset is defined as a Spark one. The 
# pandas DF will be autoconverted
dm.save("transactions", trx_pd)
trx = dm.load("transactions")

dm.has("mykey") # False
dm.save("mykey", trx)
dm.has("mykey") # True
dm.delete("mykey")
dm.has("mykey") # False
```