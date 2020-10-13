# What is it and Why We Need it?

## GCP
#### What is it?
GCP is a suite of cloud computing services that runs on same infrastructure that Google uses internally for its end-user 
prodcts

#### e.g. of other cloud services
AWS, Azure, Heroku, Oracle

#### Why we need it?
1. https://www.ibm.com/in-en/cloud/learn/benefits-of-cloud-computing
2. https://opencirrus.org/cloud-computing-important/#:~:text=Accessibility%3B%20Cloud%20computing%20facilitates%20the,of%20acquiring%20and%20maintaining%20them.

## Region
#### What is it?
Google Cloud consists of a set of physical assets, such as computers and hard disk drives, and virtual resources, such as virtual machines (VMs), that are contained in Google's data centers around the globe. Each data center location is in a region. Regions are available in Asia, Australia, Europe, North America, and South America.


## Zone
#### What is it?
Each region is a collection of zones, which are isolated from each other within the region. Each zone is identified by a name that combines a letter identifier with the name of the region


## Projects
#### What is it?
Any Google Cloud resources that you allocate and use must belong to a project. 
You can think of a project as the organizing entity for what you're building. A project is made up of the settings, permissions, and other metadata that describe your applications. Resources within a single project can work together easily, for example by communicating through an internal network, subject to the regions-and-zones rules. A project can't access another project's resources unless you use Shared VPC or VPC Network Peering.

Each Google Cloud project has the following:
* A project name, which you provide.
* A project ID, which you can provide or Google Cloud can provide for you.
* A project number, which Google Cloud provides.


## Ways to interact with the services
Google Cloud gives you three basic ways to interact with the services and resources.
1. Google Cloud Console
2. Command-line interface
    1. Cloud SDK
    2. Cloud Shell
3. Client libraries


## IAM
Identity and Access Management (IAM) lets you create and manage permissions for Google Cloud resources. IAM unifies access control for Google Cloud services into a single system and presents a consistent set of operations.\

## Cloud Storage
Cloud Storage allows world-wide storage and retrieval of any amount of data at any time. You can use Cloud Storage for a range of scenarios including serving website content, storing data for archival and disaster recovery, or distributing large data objects to users via direct download.

## Compute Engine
Compute Engine lets you create and run virtual machines on Google infrastructure. Compute Engine offers scale, performance, and value that lets you easily launch large compute clusters on Google's infrastructure. There are no upfront investments, and you can run thousands of virtual CPUs on a system that offers quick, consistent performance.

## DataProc
Dataproc is a managed Apache Spark and Apache Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning. Dataproc automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them. With less time and money spent on administration, you can focus on your jobs and your data.

## BigQuery
BigQuery is Google Cloud's fully managed, petabyte-scale, and cost-effective analytics data warehouse that lets you run analytics over vast amounts of data in near real time. With BigQuery, there's no infrastructure to set up or manage, letting you focus on finding meaningful insights using standard SQL and taking advantage of flexible pricing models across on-demand and flat-rate options.

## PubSub
Pub/Sub is a fully-managed real-time messaging service that allows you to send and receive messages between independent applications.

## Terraform
Terraform is a tool for building, changing, and versioning infrastructure safely and efficiently. 
Configuration files describe to Terraform the components needed to run a single application or your entire datacenter. Terraform generates an execution plan describing what it will do to reach the desired state, and then executes it to build the described infrastructure. As the configuration changes, Terraform is able to determine what changed and create incremental execution plans which can be applied.

