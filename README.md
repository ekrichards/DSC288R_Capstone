# DSC288R_Capstone
Flight status prediction based on historical flight data.

# Requirements
1. Python (version 3.11.x)
2. ```git```
    - Check if you have ```git``` installed by running ```git --version``` in your terminal 
    - If not, install using the following links: [Windows](https://git-scm.com/downloads/win) | [MacOS](https://git-scm.com/downloads/mac) |  [Linux](https://git-scm.com/downloads/linux)
3. ```make``` 
    - Windows
        - Install the [chocolatey package manager](https://chocolatey.org/install)
        - Run ```choco install make``` in the terminal
    - MacOS
        - Should come installed (Check if you have ```make``` installed by running ```make --version``` in your terminal)
        - If not, run ```xcode-select --install``` in your terminal
    - Linux
        - Should come installed (Check if you have ```make``` installed by running ```make --version``` in your terminal)
        - If not, it will be distribution dependent. Please find a guide online for your distribution

# Instructions
## Getting Setup
1. Clone the repo
2. Create a virtual environment (I recommend using [anaconda](https://www.anaconda.com/download)) 
and activate it
3. Run ```pip install -r requirements.txt``` in your terminal
4. Download the [Flight Status Prediction](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2018.csv) dataset from Kaggle and place the archive.zip file inside ```data\raw```
```
─ DSC288R_Capstone
└── data
    └── raw
        └── archive.zip
```
6. run ```make data``` in the terminal