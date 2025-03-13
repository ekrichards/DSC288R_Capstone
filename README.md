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
4. Run ```dvc init```
5. Download the [Flight Status Prediction](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022?select=Combined_Flights_2018.csv) dataset from Kaggle and place the archive.zip file inside ```data\raw\raw_flight```
```
─ DSC288R_Capstone
└── data
    └── raw
        └── raw_flight
            └── archive.zip
```
6. run ```make data``` in the terminal


# Data Card

![1E2173F6-1C6C-4C6E-A874-AB70C019C1F9](https://github.com/user-attachments/assets/3567a48b-3445-4ebd-83af-9583c05b146c)


# Model Cards

![4891DAF5-41C1-469B-B5BD-5FDACA650421](https://github.com/user-attachments/assets/8f259d50-d5bc-427f-b6d4-be019a7a5f90)

![4891DAF5-41C1-469B-B5BD-5FDACA650421](https://github.com/user-attachments/assets/2dfe9972-243e-4207-b992-f5794ee4e62d)

![894A7126-5DD6-408F-9EFC-0C4DE811CBBE](https://github.com/user-attachments/assets/a4d28ad2-6356-4167-9ed1-0bf2abb3b7ab)

![C3C9A85B-5CB9-4F22-9678-26454694BDAF](https://github.com/user-attachments/assets/2853ba27-be16-4778-a65a-afd23a239e62)





