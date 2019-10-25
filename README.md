<div align = "center"><img src="https://i.ibb.co/4FNYYqT/Airbender.png" alt="Airbender"  border="0"></div>

<div align="center">
<!-- Python -->
    <img src = "https://img.shields.io/badge/python-v3.6+-blue.svg"
         alt = "Python" />
<!-- Release -->
    <img src = "https://img.shields.io/badge/Release-0.1.0-00CC33.svg" 
         alt = "Release" />
<!-- Maintained -->
    <img src = "https://img.shields.io/badge/Maintained-yes-00CC33.svg" 
         alt = "Maintained" />
<!-- Build Status -->
    <img src = "https://img.shields.io/badge/Build-failing-FF0000.svg" 
         alt = "Build Status" />   
<!-- Development Status -->
    <img src = "https://img.shields.io/badge/Development-in%20progress-FF9933.svg" 
         alt = "Development Status" />  
<!-- Stability -->
    <img src = "https://img.shields.io/badge/Stability-experimental-FF9933.svg" 
         alt = "Stability" />
</div>0

<div align="center">
    <sub>Built with :heart: by 
        <a href = "https://samshowalter.github.io"> Samuel Showalter</a>
    </sub>
</div> 
<br />

Airbender allows developers to run nuanced machine learning experiments with Apache Airflow by dynamically generating a code file implementing an unbiased, efficient experiment based on a simple configuration. This allows Software Engineers and Data Scientists to have more time to think about their modeling insights, as all metrics and artifacts are tracked in Airflow and can be analyzed through its User Interface. Jump down to `Examples` for more information!


# Contents
- [Overview](#overview)
- [Dependencies](#deps)
- [Installation](#install)
- [Features](#features)
    - [Airbender Configuration](#air_config)
    - [Adding Code Functionality](#code_functionality)
- [Examples](#example)
- [Documentation](#docs)
- [FAQs](#faq)
- [Contribute](#contribute)


<a name = "overview"></a>
## Overview

Ever feel like you spend too much time setting up Machine Learning experiments and too little time analyzing model performance and insights? After you have run an experiment, do you ever lose track of intermediate data or code artifacts? Have you ever wanted to visualize all steps of your experiment for debugging purposes, or to explain your logic to others? Are you concerned your methodology may be leaking test information to your model? Airbender offers a solution to all of these issues, and more!

Airbender is a meta-programming tool that creates machine learning experiments to be run on Apache Airflow as Directed Acyclic Graphs or DAGs. Given a JSON-style configuration, including references to models and functions, Airbender will convert the configuration logic into a fully-fledged program. It ensures that there is never information leak and completely logs all execution progress, courtesy of Apache Airflow.

Apache Airflow is not a required dependency for Airbender, as this tool simply generates the code file that will create an equivalent DAG if compiled by Airflow. With that said, you can install Airflow as an optional dependency, as shown below in [installation](#install). For those who already have an Airflow environment set up, you may provide Airbender the path to your Airflow DAG folder and the experiments will be saved in that directory.

<a name = "install"></a>
## Installation

Airbender can be downloaded as a stand-alone package with pip:
```
pip install airbender
```

or using Anaconda:
```
conda install -c sshowalter airbender
```

If you want to download Airflow alongside Airbender, you can do so with the command below. Be sure to set up Airflow with all of the additional steps outlined on their [website](https://airflow.apache.org/installation.html).
```
pip install airbender[apache-airflow]
```


<a name = "features"></a>
## Features

<a name = "air_config"></a>
### Airbender Configurations
Airbender is configuration driven. While there are many different types of models and solutions that you can create, most of them share a high-level conceptual structure. The components of that structure are outlined below.
1. **`data_sources`** -- Integrating all data sources into a flat dataset
2. **`eda`** -- Exploratory Data Analysis 
3. **`splitting`** -- Splitting data (K-fold, train_test_split, etc.)
4. **`preprocessing`** -- Preprocessing data (outlier removal, imputation, etc.)
5. **`feature_engineering`** -- Feature Engineering (standardization, one-hot encoding, etc.)
6. **`modeling`** -- Training different statistical models and generating predictions for test/validation datasets
7. **`evaluation`** -- Compare the performance of different models across different success criteria

The names in bold above for each step represent the tag you must provide to the configuration you send to Airbender's `DagGenerator`. You do not have to include all of these steps (sometimes EDA may already be finished, or preprocessing is not needed) and within each step you have an immense amount of flexibility, but they keys of your JSON-style configuration must have of of the names listed above. A good template is shown below for this.

```{python}
airbender_config = {
                    "data_sources": None,
                    "eda":None,
                    "splitting": None,
                    "preprocessing": None,
                    "feature_engineering: None,
                    "modeling":None,
                    "evaluation":None
                   }

```
<a name = "code_functionality"></a>
### Adding Code Functionality

In Airbender, all functionality must have three components, a **`tag`**, **`callable`**, and **`parameters`**. The `callable` is an uninstantiated object or function, and the `parameters` provides some or all of the inputs for the callable. However, at times the `callable` also takes the `tag` as an input. Outlined below are some examples of what the tag variable can be.
1. **A succint name describing the operation**:   `{"LOG": {RandomForestClassifier: {"n_estimators": 10}}}`
    1. Note here that the tag is **`"LOG"`**, the callable is **`RandomForestClassifier`**, and the parameters are **`{"n_estimators": 10}`**. 
    2. Also note that these three elements are associated with python dictionaries in key value pairs. 
        1. Here, **`"LOG"`** :arrow_forward: **`RandomForestClassifier`** :arrow_forward: **`{"n_estimators": 10}`**
       

2. **A target column name for data operation**: `{"Sepal_Length": {impute: {"method": "median"}}}`
    1. In this case, the tag is a reference to a data column and is passed to the callable. You can have these types of tags in the `preprocessing` and `feature_engineering` sub-configurations.
 
<a name = "example"></a>
## Examples



<a name = "docs"></a>
## Documentation

<a name = "faq"></a>
## FAQs

**Does Airbender support Airflow plug-ins and optimizations like Celery, Dask, etc.?**

Yes! Airbender simply creates the experiments for Airflow to run, and therefore any plugin that can optimize Airflow is compatible. This is particularly valuable when developers have access to a pool of workers for parallel execution.

---------------------------------------

**I am a student or professional who does not need an enterprise-level solution. I just want to use it for personal projects. Is Airbender still useful for me to use?**

Yes! Airbender gives developers full control of Machine Learning experiments, and ensures that all tests are conducted in a unbiased way. All results are also meticulously stored, and Airflow's UI allows for easy explanation of an experiment to others. For these reasons and more, Airbender is an invaluable asset when completing Data Science projects.

---------------------------------------

**Can I use external functionality (sklearn, scipy, etc.) with Airbender?**

Yes! One of the amazing things about Airbender is external packages are completely supported since Airbender, at its core, is a meta-programming tool that reformats the functions and logic you provide into an Airflow ML experiment. **With that said**, there are exceptions to this rule. If your function has **artifacts** (e.g. mean and std for normalization), you will need to use the Airbender equivalent to ensure that there is no information leak between train and test datasets. Also, if a data operation **returns anything besides the transformed data**, you will need to use the Airbender equivalent there as well. 

---------------------------------------

**Is Airbender optimized for small-dataset experiments?**

No. Because Airflow has its own scheduler and keeps meticulous logging of the experimental DAG being run, it takes significantly longer to run an entire experiment and store all of the information (milliseconds to seconds, depending on the complexity of the experiment. More tasks = longer runtime). However, Airbender logs all experiments, their metadata, and any other information the developer would like to track. For this reason, it is optimized for industrial-scale, big-data machine learning solutions where the overhead for logging is negligable. 

<a name = "contribute"></a>
## Contribute

We would love for you to contribute to Airbender! Please refer to our [contribution guidelines](https://github.com/SamShowalter/airbender/blob/master/CONTRIBUTING.md) for more information.
