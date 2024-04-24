# YouTube Data Harvesting and Warehousing 

**Introduction**

YouTube Data Harvesting and Warehousing is a project aimed at developing a user-friendly Streamlit application that leverages the power of the Google API to extract valuable information from YouTube channels. The extracted data is then stored in a MongoDB database, subsequently migrated to a SQL data warehouse, and made accessible for analysis and exploration within the Streamlit app.



**Technologies and Skills**
- Python scripting
- Data Collection
- API integration
- Streamlit
- Plotly
- Data Management using MongoDB (Atlas) and SQL

**Installation**

To run this project, you need to install the following packages:
```python
pip install google-api-python-client
pip install pymysql
pip install pandas
pip install streamlit
pip install mysql-connector-python


import json
from datetime import datetime
import time
import isodate
from sqlalchemy import create_engine
```


**User Guide**

_ **Step 1: Data collection zone**
Search channel_id, copy and paste on the input box and click the Extract Data button to collect in the Data collection zone.


_ **Step 2: Data Migrate zone**
Activate Scrape and Insert Data button  to migrate the specific channel data to the MySQL database.


_ **Step 3: Channel Data Analysis zone**
Select a Question from the dropdown option you can get the results in Dataframe format
can choose which channel's data to migrate. To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables, including channels, playlists, videos, and comments, utilizing SQL queries.




