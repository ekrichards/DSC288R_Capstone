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


![A7865B96-5C07-4F7A-849F-D94577F1937C](https://github.com/user-attachments/assets/d855bb70-ece4-41c3-b5c6-d22d402f70a8)

![5A3C049B-1AE3-4839-9821-633FF82C7F33](https://github.com/user-attachments/assets/5a6ef21e-37a7-4adb-b314-37dc331733e6)
