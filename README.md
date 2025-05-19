# smart-store-huntsman

## Set Up Local Environment
```
python3 -m venv .venv
source .venv/bin/activate
```

## Folder Layout
```
smart-store-huntsman/   
│
├── data/                
│   ├── raw/   
       └──customers_data.csv
       └──products_data.csv
       └──sales_data.csv                    
│
├── scripts/   
    ├──data_preparation/
        └──prepare_customer.py
        └──prepare_products.py
        └──prepare_sales.py
                        
│
├── utils/                     
│   └── logger.py             
│
├── .gitignore                 
├── README.md                  
└── requirements.txt 
```          

## Git Add, Commit, Push to GitHub
```
git add .
git commit -m "add message"
git push
```
## Running Scripts 
### Data_Prep
```
source .venv/bin/activate
/opt/anaconda3/bin/python3 scripts/data_preparation/prepare_customers.py
/opt/anaconda3/bin/python3 scripts/data_preparation/prepare_products.py 
/opt/anaconda3/bin/python3 scripts/data_preparation/prepare_sales.py 
```

### Data_Cleaning
```
Data Cleaning
python3  -m unittest tests/test_data_scrubber.py
``