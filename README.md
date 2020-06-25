# gcp-dask-jupyter

-------------------------------------------------
### "Distributed Analytics, from Jupyter Notebook, via Dask on GKE"

##### NOTE: assumes you've either installed gcloud cli locally and setup credentials OR you're using Google Cloud Shell.
##### NOTE: replace <project-id> and <region> with you're specific values throughout the following instructions.
-------------------------------------------------

#### Jupyter Notebook

###### NOTE: There is a "<project-id>" in jupyter-gcs-bq.ipynb that needs to be replaced with your specific value.

###### NOTE: The 3 main operations within ```jupyter-gcs-bq```
- querying a BigQuery Table via ```google-cloud-bigquery```
- moving BigQuery Table data to GCS bucket (as separate csv files) via ```gcsfs```
- loading a Dask Dataframe with csv data from GCS bucket via ```dask.distributed```, ```dask.dataframe``` and ```pandas``` (used internally for dask)
###### NOTE: Run each cell individually with (it selected)```CTRL+ENTER```   
###### NOTE: Click "play" button to execute and move to next cell.
-------------------------------------------------

#### Dask Workloads

-------------------------------------------------

#### GKE Infrastructure

-------------------------------------------------

### Spin up a GKE Public Cluster

For context, this repo will outline setup instructions with gshell  https://github.com/consistent-solutions/terra-gke-jumpstart

#### Alternatively...

##### 1. Clone Repo
- ```git clone https://github.com/gruntwork-io/terraform-google-gke```
##### 2. Create ```.tfvars``` file
* ```cd terraform-google-gke```   
```
	cat << EOF > terraform.tfvars
	project = "<project-id>"
	region = "<region>"
	location = "<region>"
	EOF
```
##### 3. Terraform Cloud Infra
- ```terraform init```
- ```terraform plan```
- ```terraform apply```
-------------------------------------------------

### Copy over Dataset and Table (from public dataset to your own project)
##### 1. Visit this link: https://console.cloud.google.com/bigquery?p=bigquery-public-data&page=project
- Public datasets are within google managed project that gets “pinned” when you add it by visiting the above link.
- BigQuery provides a limited number of sample tables that you can query. These tables are contained in the ```bigquery-public-data:samples``` dataset.
- (more details: https://cloud.google.com/bigquery/public-data)

##### 2. Create Dataset
- in ```<project-id>``` project named ```new_york_taxi_trips```

##### 3. From BigQuery webui, click ```bigquery-public-data``` ####
- (below "Resources" on left side)
- then click ```new_york_taxi_trips``` Dataset
- finally, click ```tlc_yellow_trips_2018``` Table

##### 4. Click "COPY TABLE" ####
- <describe or provide picture>
- fill out form (selecting ```new_york_taxi_trips``` Dataset created in step 2 and name the new Table ```tlc_yellow_trips_2018```), and click "COPY"
-------------------------------------------------

### Provide Permissions
##### 1. Create IAM Service Account
- ```gcloud iam service-accounts create jupyter-bigquery-gcs --display-name 'jupyter-gcs-bq'```
##### 2. Create SA Credentials File
- gcloud iam service-accounts keys create jupyter-bq-gcs-cred.json --iam-account jupyter-bigquery-gcs@~~<project-id>~~.iam.gserviceaccount.com
##### 3. Add IAM Roles to Existing Service Accounts
- -----> **jupyter-bigquery-gcs@~~<project-id>~~.iam.gserviceaccount.com**
- **```BigQuery Data Owner```** =>
	- gcloud projects add-iam-policy-binding ~~<project-id>~~ --member=serviceAccount:jupyter-to-bigquery@~~<project-id>~~.iam.gserviceaccount.com --role='roles/bigquery.dataOwner'
- **```BigQuery Job User```** =>
	- gcloud projects add-iam-policy-binding ~~<project-id>~~ --member=serviceAccount:jupyter-to-bigquery@~~<project-id>~~.iam.gserviceaccount.com --role='roles/bigquery.jobUser'
- **```Storage Object Creator```** =>
	- gcloud projects add-iam-policy-binding ~~<project-id>~~ --member=serviceAccount:jupyter-to-bigquery@~~<project-id>~~.iam.gserviceaccount.com --role='roles/storage.objectCreator'
- **```Storage Object Viewer```** =>
	- gcloud projects add-iam-policy-binding ~~<project-id>~~ --member=serviceAccount:jupyter-to-bigquery@~~<project-id>~~.iam.gserviceaccount.com --role='roles/storage.objectViewer'
- -----> **example-cluster-sa@~~<project-id>~~.iam.gserviceaccount.com**
- **```Storage Object Viewer```** =>
	- gcloud projects addd-iam-policy-binding ~~<project-id>~~ --member=serviceAccount:example-cluster-sa@~~<project-id>~~.iam.gserviceaccount.com --role='roles/storage.objectViewer'

-------------------------------------------------

### Install Helm
##### 1. Install from script (https://helm.sh/docs/intro/install/)
- ```mkdir helm-install/ && cd helm-install/```
- ```curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3```
- ```chmod 700 get_helm.sh```
- ```./get_helm.sh```
-------------------------------------------------

### Install and enable Web Preview of Dask UIs & Jupyter Notebook
#### NOTE: will need to ```$ ps``` and ```$ kill #``` to free up ports afterwards
##### 1. Install Dask via Helm
- ```helm install dask-example dask/dask -n default --set jupyter.ingress.enabled=false --set webUI.ingress.enabled=
false --set jupyter.serviceType=NodePort --set scheduler.serviceType=NodePort --set jupyter.extraConfig="c.NotebookApp.allow_origin_pat='.*-dot-devshell.appspot.com$'" --set worker.env[0].name="EXTRA_PIP
_PACKAGES" --set worker.env[0].value="gcsfs"```

##### 2. Kubectl port-forward Dask UIs ```jupyter``` & ```scheduler```
- ```kubectl port-forward $(kubectl get po -n default -o name -l component=jupyter) -n default 3000:8888 &```
- ```kubectl port-forward $(kubectl get po -n default -o name -l component=scheduler) -n default 5000:8787 &```

##### 3. Kubectl cp SA credentials file to ```jupyter``` pod
- ```kubectl cp jupyter-bq-gcs-cred.json default/$(kubectl get po -l component=jupyter -n default -o name | cut -d '/' -f 2):/home/jovyan/jupyter-bq-gcs-cred.json```

#### 7) Create a GCS bucket named ```jupyter-gcs-bq```
- gsutil mb -p ~~<project-id>~~ -c STANDARD -l ~~<region>~~ -b on gs://jupyter-gcs-bq

#### 8) Open "Web Preview" on port 3000 for ```jupyter``` ui
- password is ```dask```

#### 9) Open "Web Preview" on port 5000 for ```scheduler``` ui
- dashboard of worker activity

#### 8) Clone this repo to your local machine
- drag-and-drop a local copy of ```jupyter-gcs-bq.ipynb``` into filesystem explorer (left side) of jupyter ui

-------------------------------------------------
