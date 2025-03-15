# DSC288R_Capstone
Flight status prediction based on historical flight data.

Model and data cards are available in the MODEL_CARDS.md and DATA_CARD.md mardown files respectively.

# Prerequisites
Before setting up the environment, make sure you have the following:
1. 32GB of memory and up to 16 cores
    - 24GB has been shown to work, but is not recommended. This program is very memory intensive.
    - The core count should not exceed 16 cores as multithreading is enabled. Any more than 16 cores can cause out of memory (OOM) errors.
2. Python (Recommended: v.3.11.x)
    - Download and install from one of the following links: [Windows](https://www.python.org/downloads/windows/) | [MacOS](https://www.python.org/downloads/macos/) |  [Linux](https://www.python.org/downloads/source/)
    - Verify installation by running: `python --version`
3. Git (Repository manager)
    - Download and install from one of the following links: [Windows](https://git-scm.com/downloads/win) | [MacOS](https://git-scm.com/downloads/mac) |  [Linux](https://git-scm.com/downloads/linux)
    - Verify installation by running: `git --version`
4. Anaconda (Virtual environment manager)
    - Download and install from the following link: [Anaconda](https://www.anaconda.com/download/success)
    - Verify installation by running: `conda --version`
5. VS Code or other preferred IDE

# Virtual Environment Setup
1. Using conda create a virtual environment using the included environment.yml file: `conda env create -f environment.yml`
2. If you are using a separate environment manager, prefer pip installing, or are using an environment incompatible with conda, please ensure the following packages are installed:
```
- rich
- pyyaml=6.0.1
- numpy=1.26.4
- pandas=2.2.3
- pyarrow=15.0.2
- holidays
- scikit-learn=1.4.2
- duckdb
- matplotlib=3.8.4
- seaborn=0.13.2
- kaggle==1.6.17
- requests==2.32.3
```
*Important Note: The models provided were trained using scikit-learn = 1.4.2 so you may run into trouble running the results notebooks if you use a different package version*

3. Activate your new environment using the anaconda navigator or `conda activate DSC288R_Capstone_Group09`

# Cloning the Repository
If you haven't already, clone the project repository and navigate to the root directory:
```
git clone https://github.com/ekrichards/DSC288R_Capstone.git
cd <PROJECT_DIRECTORY>
```
Replace <PROJECT_DIRECTORY> with the local location of your cloned repository

# Configuration
This project uses the Kaggle API to download the [Flight Status Prediction](https://www.kaggle.com/datasets/robikscube/flight-delay-dataset-20182022) dataset from Kaggle. 
1. Create a Kaggle account
2. Navigate to your account settings
3. Under API click **Create New Token** (this will download a `kaggle.json` file)
4. Move the `kaggle.json` to inside the config folder within the repository. It should look something like this:
```
DSC288R_Capstone/
├── config/
│   ├── data.yaml
│   ├── models.yaml
│   ├── paths.yaml
│   └── kaggle.json
```
*Important Note: Never publish your api tokens! They are essentially a username and password. We have included kaggle.json in the .gitignore for security purposes but please be mindful when using it.*

# Running the Code
The code is split into 2 main sections, the pipeline and the notebooks. The pipeline will handle data ingestion and cleaning as well as training the models. The notebooks will show the results.
### Running the Pipeline
1. Navigate to the root directory of the repository
2. Run `python pipeline.py --run`
    - This will run both the data and machine learning (ML) pipelines
    - You have the following options when using `pipeline.py`
        - Data pipeline only: `python pipeline.py --data`
        - ML training pipeline only: `python pipeline.py --train all`
        - ML base model training pipeline only: `python pipeline.py --train all --base`
        - ML hyperparameter tuning pipeline only: `python pipeline.py --tune all`
        - If you wish to run specific models or subsets of models, replace `all` with the following:
            - `lin_reg` for [Linear Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LinearRegression.html)
            - `sgd_reg` for [Stochastic Gradient Descent Regressor](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDRegressor.html)
            - `hgb_reg` for [Histogram Gradient Boosting Regressor](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingRegressor.html)
            - `mlp_reg` for [Multi-Layer Perceptron Regressor](https://scikit-learn.org/stable/modules/generated/sklearn.neural_network.MLPRegressor.html)
            - `reg` for all regression models
            - `log_clf` for [Logistic Regression](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html)
            - `sgd_clf` for [Stochastic Gradient Descent Classifier](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.SGDClassifier.html)
            - `hgb_clf` for [Histogram Gradient Boosting Classifier](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.HistGradientBoostingClassifier.html)
            - `mlp_clf` for [Multi-Layer Perceptron Classifier](https://scikit-learn.org/stable/modules/generated/sklearn.neural_network.MLPClassifier.html)
            - `clf` for all classifier models

*Important Note: The entire pipeline can take up to 4 hours to run so plan accordingly.*

### Running the Notebookes
1. Navigate to `DSC288R_Capstone/notebooks`
```
DSC288R_Capstone/
├── notebooks/
│   ├── Classification.ipynb
│   ├── Regression.ipynb
│   └── EDA.ipynb
```
2. Run `Classification.ipynb` for classification model results
3. Run `Regression.ipynb` for regression model results
4. Run `EDA.ipynb` for EDA performed

*Important Note: We use imputation importance to determine feature importance, these last cells can take 15 minutes to run.*

# Troubleshooting the Pipeline
Due to the data set size and long training times, there is a chance that things can break.
### Pipeline Errors
- Running all the code from within the integrated terminal of your preferred IDE will usually ensure correct paths are called.
- Check that you are in the correct environment.
- Please ensure your `kaggle.json` is in the correct place.
### OOM Errors
- Double check your memory and core counts before running the code.
- Restart your kernel and ensure you have nothing else running when allowing the script to run. You may need to restart your computer or instance.
### Library Errors
- Ensure you are using the correct package versions as described above.
- `pip install` does not handle dependencies well, if you are finding package versioning incompatibility with your environment when running the code, please consider using conda.
### Download Files
- If you cannot run the pipeline, we have provided the completed training data, testing data, and models: https://drive.google.com/file/d/1DxrDst-ysqBzhPkIkGl5iFm7Z1CYPav3/view?usp=drive_link
- Place the parquet files within `DSC288R_Capstone/data/final`. It should look something like this:
```
DSC288R_Capstone/
├── data/
│   ├── provided/
│   ├── raw/
│   ├── extracted/
│   ├── processed/
│   └── final/
│       ├── train_data.parquet
│       └── test_data.parquet
```
- Place all model folders within `DSC288R_Capstone/models`. It should look something like this:
```
DSC288R_Capstone/
├── models/
│   ├── lin_reg/
│   ├── sgd_reg/
│   ├── hgb_reg/
│   ├── mlp_reg/
│   ├── log_reg/
│   ├── sgd_clf/
│   ├── hgb_clf/
│   └── mlp_clf/
```
- You should now be able to run the notebooks.

*Important Note: You will still need sufficient memory to run the models this way.*