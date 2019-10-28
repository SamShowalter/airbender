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
</div>

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
    - [Opoerator Families and Sequencing](#op_families)
    - [DAG Layers](#dag_layers)
    - [Conceptual vs. Physical DAG Layers](#conc_phys_dag_layer)
    - [Execution Order](#exec_order)
- [Examples](#example)
    - [Iris Classification](#iris_overview)
        - [Imports](#iris_imports)
        - [Reading in Data](#iris_rd)
        - [Exploratory Data Analysis](#iris_eda)
        - [Splitting Data](#iris_split)
        - [Preprocessing](#iris_p)
        - [Feature Engineering](#iris_fe)
        - [Modeling](#iris_m)
        - [Evaluation](#iris_e)
        - [Consolidate Configuration](#iris_cc)
        - [Generate Airbender DAG](#iris_gen)
        - [View and Run DAG in Airflow](#iris_dag)
    - [Employee Attrition](#attr)
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

The names in bold above for each step represent the tag you must provide to the configuration you send to Airbender's `DagGenerator`. You do not have to include all of these steps (sometimes EDA may already be finished, or preprocessing is not needed) and within each step you have an immense amount of flexibility, but they keys of your JSON-style configuration must have of of the names listed above. A good template is shown below for this, just replace the `None` variable with your configuration for each step. 

```python
airbender_config = {
                    "data_sources": None,
                    "eda":None,                        #Not yet supported
                    "splitting": None,
                    "preprocessing": None,
                    "feature_engineering": None,
                    "modeling":None,
                    "evaluation":None
                   }

```
<a name = "code_functionality"></a>
### Operators: Adding Code Functionality

In Airbender, all operators must have three components, a **`tag`**, **`callable`**, and **`parameters`**. The `callable` is an uninstantiated object or function, and the `parameters` provides some or all of the inputs for the callable. However, at times the `callable` also takes the `tag` as an input. Outlined below are some examples of what the tag variable can be.
1. **A succint name describing the operation**:   `{"LOG": {RandomForestClassifier: {"n_estimators": 10}}}`
    1. Note here that the tag is **`"LOG"`**, the callable is **`RandomForestClassifier`**, and the parameters are **`{"n_estimators": 10}`**. 
    2. Also note that these three elements are associated with python dictionaries in key value pairs. 
        1. Here, **`"LOG"`** :arrow_forward: **`RandomForestClassifier`** :arrow_forward: **`{"n_estimators": 10}`**
       

2. **A target column name for data operation**: `{"Sepal_Length": {impute: {"method": "median"}}}`
    1. In this case, the tag is a reference to a data column and is passed to the callable. You can have these types of tags in the `preprocessing` and `feature_engineering` sub-configurations.
    
<a name = "op_families"></a>
### Operator Familes and Sequencing

In some cases, you may want to conduct several operations on the same data in sequence. For example, in your dataset you might have a column that has outliers, missing data, and needs to be standardized with a linear transformation. For these tasks the output of one is the input for another, and all must run in a sequence, as opposed to parallel execution.

To accommodate this, we build on the `tag`, `callable`, and `parameters` concept we introducted previously to create **operator families**. An operator family that runs through the sequence above for `Sepal_Length` would look like this:

```python
{
 "Sepal_Length": {
                  remove_outliers: {"threshold": 6},
                  impute: {"method": "median"},
                  linear_transformation: {"method": boxcox}
                 }
}
```

As you can see, we have mapped three operators to the same tag and created an operator family. Though Python dictionaries do not have ordering, Airbender will lock these operators into place and execute each operator in the order it was written. The output of the first operation will be fed into the second using Airflow's `XCOM` functionality, and the process will continue  until all operators have been run.  

<a name = "dag_layers"></a>
### DAG Layers

Now that we have an effective way of writing sequences of operations that apply to the same data as operator families, we need a way to articulate which operator families can run in parallel. This is done using the `DagLayer` object. Dag layers are collections of operator families. 

When it is necessary, like with column-wise operators in `feature_engineering`, the `DagLayer` will provision itself into sublayers to ensure that the final DAG will connect correctly. Before modeling can begin, all of the engineered features need to be collected into a single dataset. The need for a merge operation is programmatically determined, and added as a sublayer.

DAG Layers will also determine their own head and tail pointers, and therefore can accommodate collections of operator families of varying length. To that end, an important note about writing Airbender configurations in that you must specify which selections of a configuration are DagLayers. To convert our operator family above into a DagLayer, we simply wrap our operator family with the DagLayer object. This will automatically validate your configuration, and if there are errors it will explain in detail what the problem is for easy debugging.

```python
DagLayer(
    {
     "Sepal_Length": {
                      remove_outliers: {"threshold": 6},
                      impute: {"method": "median"},
                      linear_transformation: {"method": boxcox}
                     }
    }
)
```

<a name = "conc_phys_dag_layer"></a>
### Conceptual vs. Physical DAG Layers

Lastly, there is a difference between a conceptual DAG layer and a physical DAG layer. The items listed above in the high-level  structure represent the only conceptual layers that can be considered by Airbender. However, you can add as many physical DagLayers as you want within these conceptual layers. You can even store DagLayers in lists. All three configurations below, outlined for the feature_engineering section, are valid structures for Airbender.

**Configuration 1:** Adding subsections to a conceptual DAG layer

```python
'feature_engineering':

    { 
        'column_transformations': DagLayer(...),
        'bulk_transformations':   DagLayer(...),
        # ...
    }
    
```

-------------------------------

**Configuration 2:** Lists of DagLayers

```python
'feature_engineering':

    { 
        "transformations": [DagLayer(...),      # Column transformations
                            DagLayer(...),      # Bulk transformations
                            # ...
                           ]
    }
    
```

-------------------------------

**Configuration 3:** Nested, Composite DagLayers

```python
'feature_engineering':

    { 
        "NLP":
              {"tokenization": DagLayer(...),
               "tagging": {
                              "named_entity_recognition": DagLayer(...),
                              "parts_of_speech":          DagLayer(...)
                       
                          },
               "lemmatization": DagLayer(...)
              }
        
            
        "power_transformations": [DagLayer(...),      
                                  DagLayer(...),      
                                  ...]
    }
    
```

<a name="exec_order"></a>
### Execution Order

While the order of conceptual DAG layers is set by the execution configuration, physical DagLayer order is taken as-is from within each conceptual layer. Order is determined top-to-bottom and depth first. All nested DagLayers that belong to a single key will be ordered before the DagLayers of the following key. Please see the code and comments below for an example using **configuration 3**.

```python
'feature_engineering':

    { 
        "NLP":
              {"tokenization": DagLayer(...),                                #First
               "tagging": {
                              "named_entity_recognition": DagLayer(...),     #Second
                              "parts_of_speech":          DagLayer(...)      #Third
                       
                          },
               "lemmatization": DagLayer(...)                                #Fourth
              }
        
            
        "power_transformations": [DagLayer(...),                             #Fifth                    
                                  DagLayer(...),                             #Sixth
                                  ...]                                       #...
    }
    
```


_We do not recommend editing the execution configuration._
 
<a name = "example"></a>
## Examples

<a name = "iris_overview"></a>
### Example 1: Iris Classification 
----------------------------------------------
Now that we have a good understanding of airbender's structure, let's consider an example with the Iris flower dataset. The dataset includes four features, listed below, and three classes we will try to predict. The goal is to build a model that can effectively determine the breed of the flower using only the length and width metrics provided in the feature set. 

**See full code for experiment [here]()**

```
Iris plants dataset
--------------------

**Data Set Characteristics:**

    :Number of Instances: 150 (50 in each of three classes)
    :Number of Attributes: 4 numeric, predictive attributes and the class
    :Attribute Information (Features):
        - sepal length in cm
        - sepal width in cm
        - petal length in cm
        - petal width in cm
    :class (type of flower):
        - Setosa
        - Versicolour
        - Virginica
```
<a name = "iris_imports"></a>
#### Imports
----------------------------------------------

Before we start, let's import Pandas and Airbender's `DagLayer` class so we can validate our configurations.

```python
from airbender.dag.layers import DagLayer
```


<a name = "iris_rd"></a>
#### Reading in Data
----------------------------------------------

Since this implementation is small and fairly simple, all we need to do is import a single dataset. Airbender has provided a link to the dataset below. To incorporate data into the experiment, we only need to write the following:

```python
data_sources = {'iris':         #Tag
   DagLayer(
            {
             'https://raw.githubusercontent.com/SamShowalter/airbender/master/tutorials/iris/airbender_iris_demo.csv': \
             {pd.read_csv: {'sep': ','}},
            }
           )
              }
```


<a name = "iris_eda"></a>
#### Exploratory Data Analysis
----------------------------------------------

While there are methods of incorporating EDA into Airbender, these are not yet supported. Moreover, Airbender primarily functions as an experimentation tool for developers to use _after_ they have done EDA. Its focus is for quickly optimizing feature engineering and modeling with unbiased experiments. 

You can view EDA for the Iris example [here]((https://github.com/SamShowalter/airbender/blob/master/tutorials/iris/airbender_iris_tutorial.ipynb))

<a name = "iris_split"></a>
#### Splitting Data
----------------------------------------------

One of the largest benefits of Airbender is the control it offers to ensure unbiased experimentation. Right after the data is read into Airbender, it is split into train and test datasets. For every preprocessing and feature_engineering operation, the train dataset is operated on first. If the operation has any artifacts (e.g. median imputation takes the median of the training dataset as its imputation value), those values are passed to the testing dataset operators and applied directly. In this way, the experiment is much more likely to be free of information leak.

Right now, Airbender only accommodates traditional train test splits with a single test slice. K-fold cross validation is coming as a new feature shortly, as is the ability to select a validation slice of data.

For our Iris example, we will take a random, 25% slice of the data for our testing set. The configuration is outlined below.


```python
#Import splitting functionality
from airbender.static.splitting import train_test_split

splitting = {'split':

                    DagLayer({'sklearn': {train_test_split: {"target": "flower_label",
                                                            "test_ratio": 0.25,
                                                            "random_state": 42}
                                         }
                            })
            }
```

<a name = "iris_p"></a>
#### Preprocessing
----------------------------------------------

In this edited dataset, we can see that there are some missing values. The original Iris dataset did not have missing values, but we have added them artificially to better simulate an actual dataset.

To accommodate these missing values, we will use median imputation, provided by airbender. We need to use Airbender's label encoder function as label_encoder's ensure we attribute the same numeric label for each class across the train and test datasets.

```python
#Imports
from airbender.static.preprocessing import impute

preprocessing = {'missing_data':

                            DagLayer({
                                        # tag name             # Operator Family
                                        'median_impute':       {impute: {'method': 'median'}}
                                    })
                }


```

<a name = "iris_fe"></a>
#### Feature Engineering
----------------------------------------------

Feature engineering is the DagLayer where you can apply functions or series of function to specific data columns. You can also pass through columns that are already in a suitable modeling format by simply putting None in place of the operator and parameters dictionary.

Based on our EDA, we noticed that `petal_length` appears to have one or more outliers. We will handle those by winsorizing them with 5%-95% bounds, then normalize the data. To demonstrate how to simply pass columns through to modeling, we will assume `sepal_length` and `petal_width` do not require feature engineering.

Lastly, we will need to encode flower_label.

```python
#Feature Engineering Imports
from airbender.static.feature_engineering import normalize_values, winsorize, encode_labels

feature_engineering = {'col_transformations':

                            DagLayer({
                                        # Column name             # Operator Family

                                        'sepal_width':            {normalize_values: None},
                                        'sepal_length':           None,
                                        'petal_length':           {winsorize:            {'limits': [0.05, 0.05]},
                                                                   normalize_values:     None},
                                        #Pass-through
                                        'petal_width':            None,
                                        'flower_label':           {encode_labels: None}
                                    })
                      }
```


<a name = "iris_m"></a>
#### Modeling
----------------------------------------------

The modeling sublayer currently follows a `fit` and `predict` interface, meaning all `scikit-learn` models and many additional algorithms like `lightgbm` and `xgboost` are compatible. Deep learning models are not yet compatible. Airbender currently only supports supervised algorithms as well. You do not need to instantiate the models you provide. Airbender will do this for you.

For the Iris dataset, we will examine three models, all from `scikit-learn`: Random Forest, Support Vector Machine, and Logistic Regression. The configuration for this is shown below.

```python
#Import models from Sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

modeling = {'modeling': 
                
                DagLayer({
                            'LOG': {LogisticRegression:         {'solver':'lbfgs'}},           
                            'RF':  {RandomForestClassifier:     {'n_estimators': 10}},
                            'SVM': {SVC:                        {'kernel': 'linear'}}
                        })  
           }
```

<a name = "iris_e"></a>
#### Evaluation
----------------------------------------------

Lastly, we need a way to determine which model is best suited to predict on this dataset. Therefore, we will provide our Airbender evaluation DagLayer with performance metrics.

```python
#Import evaluation metrics
from sklearn.metrics import precision_score, recall_score, accuracy_score, f1_score

evaluation = {'metrics':
                    
                     DagLayer({
                                'acc':            {accuracy_score:      None},
                                'recall':         {precision_score:     {'average': 'weighted'}},
                                'precision':      {recall_score:        {'average': 'weighted'}},
                                'f1':             {f1_score:            {'average': 'weighted'}}
                             })
             }
```

<a name = "iris_cc"></a>
#### Consolidating the Configuration
----------------------------------------------

Now that we have all of the functionality we need to run an experiment with the Iris dataset, we need to consolidate that into a single configuration object. This is typically done with the following structure. Note, you can write these steps in any order, as the conceptual DAG configuration will ensure correct order (e.g. `data_sources` is first, `evaluation` last).

```python
iris_config = {
                'data_sources':            data_sources,
                'splitting':               splitting,
                'preprocessing':           preprocessing,
                'feature_engineering':     feature_engineering,
                'modeling':                modeling,
                'evaluation':              evaluation
              }
```

Almost done! We need to add a few final arguments to label our experiment and send the correct metadata to developers and users. First, we need to give the experiment a `dag_name`, shown below. The `dag` argument is a list of the parameters the user wants to pass directly to airflow about the management of its execution. More information about this section can be found in the Airflow documentation [here]().

```python
airbender_config = { 
                        'dag_name': "Airbender_Iris_Tutorial",
                        
                        'dag':      {
                                        'owner': 'airbender',
                                        # 'email': [<EMAIL>, <EMAIL>, ...],
                                        # 'op_args':{},
                                        # 'op_kwargs': {},
                                    },
                                    
                        #DAG configuration we just created
                        'config' : iris_config
                   }
```

All finished! Now we are ready to generate the code for our Airbender DAG.

<a name = "iris_gen"></a>
#### Generate the Airbender DAG
----------------------------------------------

Once we have written a valid Airbender configuration, generating the code for the DAG is incredibly simple. All you need to do is give the configuration to Airbender's DAG generator, shown below.

**`Input`**
```python
#Imports
from airbender.dag.generator import DagGenerator

#Generate File
dg = DagGenerator(airbender_config)
dg.generate_file()
```

--------------------------------------------------
**`Output`**
```
Displaying Ordered Dag Layers with Tags:

0 ['data_sources', 'iris']
1 ['splitting', 'split']
2 ['preprocessing', 'missing_data']
3 ['feature_engineering', 'col_transformations']
4 ['modeling', 'modeling']
5 ['evaluation', 'metrics']

Generated Airbender file with name: Airbender_Iris_Tutorial_airbender_10-28-2019--14.11.49.py
```

Now our experiment has been converted into a DAG that is ready to run on Airflow. Note that Airbender intelligently imported all of the functions and classes we used in our configuration into the final file. You can view the file we just generated [here](https://github.com/SamShowalter/airbender/blob/master/tutorials/iris/Airbender_Iris_Tutorial_airbender_10-28-2019--15.26.01.py). 


<a name = "iris_dag"></a>
#### View and Run DAG in Airflow
----------------------------------------------

At this point, our job is finished. All we need to do is place this file into our airflow dag directory, turn on Airflow's scheduler and webserver, and watch the experiment run. While the experiment is running, you can track the progress of the DAG in real-time by with Airflow's tree or graph view, shown below.

**Generated Airbender DAG**
<img src="https://i.ibb.co/dfkLgTg/iris-dag.jpg" alt="iris-dag" border="0">
    
You can also analyze the content, input, output, and metadata of specific tasks by clicking on them and viewing the Airflow log. Airbender wraps all of your functions in decorators to ensure compatibility, but the task id will indicate which functions were run. If more information is needed, you may find it in the `params` log after clicking on a specific task instance, shown below.

**Airbender Task Viewer in Airflow**
<img src="https://i.ibb.co/4W7sB0z/iris-dag-task.jpg" alt="iris-dag-task" border="0">


### Example 2: Employee Attrition
----------------------------------------------

The Iris classification example is a relatively simple machine learning problem, even with the created data imperfections. For a more involved implementation of Airbender, please see the employee attrition data example, which leverages a synthetic dataset on Employee Attrition created by IBM.

You can view the tutorial [here](https://github.com/SamShowalter/airbender/blob/master/tutorials/attrition/airbender_attrition_tutorial.ipynb)

<a name = "docs"></a>
## Documentation

For granular documentation of the Airbender API, please visit our readthedocs.io page [here]().

<a name = "faq"></a>
## FAQs

**Does Airbender support Airflow plug-ins and optimizations like Celery, Dask, etc.?**

Yes! Airbender simply creates the experiments for Airflow to run, and therefore all plugins for Airflow are compatible. This is particularly valuable when developers have access to a pool of workers for parallel execution.

---------------------------------------

**I am a student or professional who does not need an enterprise-level solution. I just want to use it for personal projects. Is Airbender still useful for me to use?**

Yes! Airbender gives developers full control of Machine Learning experiments, and ensures that all tests are conducted in a unbiased way. All results are also meticulously stored, and Airflow's UI allows for easy explanation of an experiment to others. For these reasons and more, Airbender is an invaluable asset when completing Data Science projects.

---------------------------------------

**Can I use external functionality (sklearn, scipy, etc.) with Airbender?**

Yes! One of the amazing things about Airbender is external packages are completely supported since Airbender, at its core, is a meta-programming tool that reformats the functions and logic you provide into an Airflow ML experiment. **With that said**, there are exceptions to this rule. If your function has **artifacts** (e.g. mean and std for normalization), you will need to use the Airbender equivalent to ensure that there is no information leak between train and test datasets. Also, if a data operation **returns anything besides the transformed data in Pandas Series format**, you will need to use the Airbender equivalent or convert your operation to align with these constraints. 

---------------------------------------

**Is Airbender optimized for small-dataset experiments?**

No. Because Airflow has its own scheduler and keeps meticulous logging of the experimental DAG being run, it takes significantly longer to run an entire experiment and store all of the information (milliseconds to seconds, depending on the complexity of the experiment. More tasks = longer runtime). However, Airbender logs all experiments, their metadata, and any other information the developer would like to track. For this reason, it is optimized for industrial-scale, big-data machine learning solutions where the overhead for logging is negligable. 

<a name = "contribute"></a>
## Contribute

We would love for you to contribute to Airbender! Please refer to our [contribution guidelines](https://github.com/SamShowalter/airbender/blob/master/CONTRIBUTING.md) for more information