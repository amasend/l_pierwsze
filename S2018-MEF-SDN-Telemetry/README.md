# SDN-Telemetry Introduction
The goal of this LCCN project is to provide a platform to collect telemetry data from an SDN network and run analytics on the data.

This readme file is a guide which will help you get started, run the different components of the project, collect metrics and write analytics.

Students: Idan Yadgar & Omri Huller  
Supervisor: Jalil Morany


# Overview
## SDN simulation & Collect metrics
We used mininet to simulate the SDN, and created a default topology that includes 3 hosts, 4 switches and 1 controller.  
The controller is a ryu controller. We used python, and ryu seemed like a good choice for working with OpenFlow.

Our controller is not a standard controller. Basically, it doesn't communicate with our switches but just listens and waits for commands.  
We created a CLI that communicates with our controller. This CLI allows to install and uninstall different applications.  
Each application collects a different metric (such as byte count, packet count, etc) and one can start and stop collecting the metrics dynamically by installing and uninstalling those applications.

The output directory is watched by a different python script which gets notified when a file is created or modified in this directory.  
When a change is detected, this application pushes the new changes to PNDA and truncates changed file.

## Writing and running analytics
PNDA is a scalable platform to perform big data analytics, which combines different software components together to create a scalable system which can run as distributed cluster on many servers.

PNDA uses Apache Kafka as its entrance gate. Kafka provides a distributed publisher-consumer platform, which can be divided to different topics.  
Each producer can choose a topic and send data into it, and all the consumers of this topic will get this data. Of course, producers and consumers can be each on a different machine.

The same script that pushes the metrics to PNDA is actually a Kafka producer that sends the metrics to PNDA using this mechnism.

PNDA also allows using Apache Spark for data analysis. Apache Spark is a scalable distributed system that allows analysing big data.  
We used spark with python in order to run some analytics on the collected metrics.

The analytics are written and ran inside Jupyter notebooks.  
Jupyter is also included in PNDA. It provides a web interface for writing and running documents that contain live code.  
We created a notebook for each analytic script we wrote and used Spark inside the notebook to manipulate and collect the data.  
Also, in a Jupyter notebook, we can use different python libraries in order to, for example, plot out results.


# Installing dependencies
In order to simulate the network and collect the metrics, we recommend using Ubuntu - but any other distribution should be good.  
In this section we will use the Ubuntu commands in each step.

## Download the project source
Start by cloning this repository to your machine.

Install git: `sudo apt install git`

Clone the repository `git clone https://gitlab.cs.technion.ac.il/lccn/S2018-MEF-SDN-Telemetry.git`

Navigate to the repository directory: `cd S2018-MEF-SDN-Telemetry`


## Install mininet
Start by installing mininet: `sudo apt install mininet`

Deactivate openvswitch-controller by running:  
`sudo service openvswitch-controller stop`  
`sudo update-rc.d openvswitch-controller disable`

Now try to run a mininet test: `sudo mn --test pingall`

If there is an error, try installing mininet via this guide: http://mininet.org/download/  
Try **Option 3: Installation from Packages**.


## Install ryu
Start by installing pip: `sudo apt install python-pip`

Now install ryu: `pip install ryu`


## Install Watchdog
Watchdog is a python library for watching files for modifications, run th following:  
`pip install watchdog`

## Install Kafka for python
Install kafka python: `pip install kafka-python`


# Install Red PNDA
Red PNDA is a smaller version of PNDA. It has less capabilities but contains Kafka, Spark and Jupyter.  
Red PDA is easier to install, as it comes as an image that can be run on a VM.

If you did the last section on a VM, red PNDA can be installed on a different VM, it doesn't have to be a VM inside the machine used for the last section.  
However, the two machine must be able to reach each other and establish a connection.

Download the image [from here](http://d5zjefk3wzew6.cloudfront.net/Red-PNDA_0.2.8.ova).

Install it on VMWare or VirtualBox, with at least 4GB of RAM, 2 CPUs and 8GB of storage. 

After installing and running the machine, use the username *pnda* and password *pnda* to log in.

Now, run the following: `sudo bash assign-ip.sh eth0`.  
Get the IP of the machine by running `ifconfig`.  
Open your web browser and navigate to this IP, you should see PNDA console (a web interface).

If you have any problems with this section, read [this guide](https://github.com/pndaproject/red-pnda/blob/develop/README.md).


# Project Structure

Go back to the machine where you downloaded the project's source code and navigate to the directory thar contains the source code of the project.

In the next section you will learn the project structure and the different components.

## The controller
We defined our controller inside the `controller/loader.py` file.

The controller is a Ryu application.  
Our controller is notified when a new switch is found in the network, and the `switch_features_handler` method is executed when this happends because we decorated it with:  
`@set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)`

At the begining we don't have any of our metric-collectors installed and loaded. Thus, we save all the events of new switches inside a list, and we pass this list to each metric-collector - this way the metric collector knows which switches are in the network.

Also, in order to install and uninstall applications, our controller listens on port 9999 for the CLI connection and waits for commands.

Go to the `controller` directory and run `ryu-manager loader.py` in order to start the controller.

## Our topology
Out topology has 3 hosts, 4 switches and 1 remote controller.  
In the `defaultTopo.py` file we define and run our network using mininet.

Run this script as a super-user (`sudo python defaultTopo.py`) and you should get the mininet CLI.  
Execute `nodes` and `links` commands in order to see the different nodes and links in the network.

We used a remote controller, so mininet will connect to our controller if it is online.

## The metric-collectors
Inside the `controller/apps` directory we have out applications we can load and unload dynamically.  

We can see that our packet logger class is wrapped with a function. this is done in order to pass in parameters.  
Because we don't instantiate the class instance ourselves, but send it to Ryu to load it, we cannot control the parameters that are passed to the contructor, so we chose this approach to pass in parameters.

Each application receives the list of new-switch events from the controller, the table id and the arguments that the user sent from the CLI.  
The reason each app receives the table id to work with, is to allow many metric collectors to work independently. The controller is responsible to send the table IDs in a way that different applications don't collide with each other, and each metric collector is responsible to sent the packet to the next table (to table ID + 1).  
That way, each application adds entries to a different table, and each entry is responsible to pass to matched packet to the next table.

## The CLI
The CLI is used to install and uninstall applications on the controller dynamically.  
It can be found inside `controller/cli.py`.

In the CLI we firstly connect to the controller at port 9999.
After that, we install the learning switches application, which allows the switches to learn the hosts in each of their port, and to create their "routing tables".

Go to the `controller` directory and run `python cli.py` in order to start the CLI.  
Enter `?` to view the available commands.

Use `install` to load a metric collector and `uninstall` to unload one. Use `list` to see all the supported apps.

### **Note:**
In our project we support LIFO install/uninstall order.

This means that this order is okay:  
`install PacketCount 4`  
`install PacketLog`  
`uninstall PacketCount`  
`uninstall PacketLog`

But this will not work:  
`install PacketCount 4`  
`install PacketLog`  
`uninstall PacketLog`  
`uninstall PacketCount`



## Our watchdog
Inside the `out_watchdog.py` file we have our watchdog that watches the destination directory.  
It also implements a Kafka producer, and sends the data to PNDA.
The kafka server, the topic and the path to watch can be modified using the variables on the top.

**-- Change the Kafka server address to match the Red PNDA address --**

Run `python out_watchdog.py` in order to start the watchdog.

## The example notebooks
Inside `example_notebooks` directory we have some example notebooks. We will use them later.


# Collecting analytics
Start red PNDA, run mininet, the controller, the CLI and the watchdog.  
Now, after doing so, we are ready to collect metrics from our network.

## Install a metric collector
Install the packet count application by typing in `install PacketCount 4` inside the CLI.  
This command installs the packet count metric collector, and sends the parameter 4 to it. Packet count recieves a parameter which sets the interval in seconds. Sending 4 means that the packets will be counted on each switch, and after every 4 seconds, the amount of packets will be logged to the destination directory and the counters will be set back to 0.

## Run some traffic
Now run some traffic, for example by typing `pingall` in the mininet CLI. This will send pings between all the hosts.

## Check Red PNDA to see the metrics
After the metrics have been collected by the metric collector and saved inside the destination directory, the watchdog reads them and sends them to Red PNDA.

The data is sent to the "raw" topic. Everything sent to this topic is automatically saved under /data/year=Y/month=M/day=D/hour=H/dump.json  
Where Y/m/d is the date and H is the hour when the data has arrived.

## Run analytics
In your browser, in the PNDA console, navigate to `Jupyter`. the username and password are `pnda` / `pnda`.

Inside Jupyter you will some notebooks that come with red PNDA. They include some example and guides.

Also, you can upload notebooks. Select one of the notebooks inside the `example_notebooks` directory.  
Inside the notebook you will see some code sections. You can run each section by pressing `ctrl+Enter` or `shift+Enter`.

Change the year, month, day and hour on the top of the document to match your data.


# Write your own code
## Create new metric collectors
In order to create a new metric collector, clone one of the existing ones and change the class and function names.  
Go to `controller/loader.py` file and import it, and go to `controller/cli.py` and add it to the `argsByApp` dictionary.

You can implement the `start` method to run some code when the app is loaded and basically write a standard Ryu app.  
It is important to use the table ID you recieved from the controller to prevent unusual behavior.

## Create new analytics
Open Jupyter inside the PNDA console in the browser and create a new notebook. Choose `PySpark` notebook in order to work with Apache Spark inside your document.

You can use any of the APIs that come with Spark and import and use any library that comes with Jupyter.


# Moving to the real PNDA
As we said, red PNDA is a smaller, lightweight version of PNDA.  
Red PNDA was designed to run a laptop and to be used for educational, development and experimental matters.

## Our try
We wanted, as the next step, to change from red PNDA to the real PNDA.  
PNDA is designed ro run in a distributed environment, on a cluster of servers, providing load balancing and ease of scaling.

We tried to simulate a cluster, by running 2 different virtual machines on our laptop, and used [this guid](http://pnda.io/pnda-guide/provisioning/server-cluster/PREPARE.html) to install PNDA.  
Basically, we can divide it to two parts: installing PNDA with its components & intalling the PNDA CLI which allows us to control our clusters.

Unfortunately, we had some issues while trying to install PNDA on such an architecture (two VMs, one for PNDA and one for the CLI).  
We recieved many different build errors, and even after succeeding to install everything properly, we had errors while creating the cluster with the CLI.

## Update - connecting to MefNet PNDA
First - need to generate a key pair using ssh-keygen  and send the public key to Donald (Cisco) - so he adds this public key to authorised keys on panda-server1.cisco.com

Now -You should now be able to connect with ssh, something like this:

ssh -i your-private-key.pem -D:5000 ciscopanda@panda-server1.cisco.com

Then you can connect to the PNDA cluster using a browser that uses localhost:5000 as a SOCKS proxy. I use Firefox because it supports DNS resolution via the SOCKS proxy, which is very convenient. This is the “Network Proxy” settings page in Firefox – note the checkbox at the bottom to “Proxy DNS when using SOCKS v5”.
After you have configured Firefox to use SSH as the SOCKS proxy, you can connect to the PNDA dashboard: http://pnda-cdh-edge/ 

If you need some help navigating and using PNDA then I can give an overview for you, via Webex.

If you want to upload data into the PNDA HDFS datastore then it should be possible to directly use the HDFS console: http://pnda-cdh-mgr-1:8888/filebrowser/ (pnda / pnda) 

If you want to stream data into PNDA then I can recommend sending it via a PNDA gateway host that is hosted in another part of MEFnet. We use it to stream telemetry from TATA and TI-Sparkle.



## Our colnclusions
It seems like PNDA is not designed to run on such an architecture, but on a real cloud/server cluster.

PNDA can be installed on many environments, one for example can be AWS which we recommend.  
You can follow the steps in [this link](http://pnda.io/pnda-guide/provisioning/aws/PREPARE.html) to create your cluster and install PNDA.

## Configure watchdog to use new Kafka servers
After installing PNDA, you should change the *kafka_server* variable in the **watchdog** to be a python list containing all the IPs of your kafka servers (with red PNDA we had only one).

Also, change the topic to the topic you want the data to be sent to.

## Working with PNDA
Working with PNDA should be the same like you did with red PNDA, but now the **watchdog** will have a cluster of kafka servers to send the data to.

The only change will be the source from which you will read the data when writing analytics with Jupyter Notebook, according to the topic you sent your data to.  
You can read further [here](http://pnda.io/pnda-guide/streamingest/topic-preparation.html), mainly the **Dataset location**.
