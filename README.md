# CreditCardFraudDetection
This project uses - Hive-Hbase integration , Oozie workflows with coordinator.
As a big data engineer, architecting and building a solution to cater to the following requirements:

 

Fraud detection solution: This is a feature to detect fraudulent transactions, wherein once a cardmember swipes his/her card for payment, the transaction should be classified as fraudulent or authentic based on a set of predefined rules. If a fraud is detected, then the transaction must be declined. Please note that incorrectly classifying a transaction as fraudulent will incur huge losses to the company and also provoke negative consumer sentiment. 
Customers' information: The relevant information about the customers needs to be continuously updated on a platform from where the customer support team can retrieve relevant information in real time to resolve customer complaints and queries

The data from the several POS systems will flow inside the architecture through a queuing system like Kafka. The POS data from Kafka will be consumed by the streaming data processing framework to identify the authenticity of the transactions.

 

Note: One of the SLAs of the company is to complete the transaction within 1 second. Hence, the framework should be accordingly chosen to facilitate this SLA.

 

Once the POS data enters into the Stream processing layer, it is assessed based on some parameters defined by the rules. Only, when the results are positive for these rules, the transaction is allowed to complete. Now, what are these rules? How can one obtain these parameters and where are they stored? These questions will get answered in some time.

 

Once the status of a transaction is determined as a “Genuine” or “Fraudulent”, the details of the transaction, along with the status, are stored in the card_transactions table. 

 

Now, let’s understand the various parameters defined by the rules required to determine the authenticity of the transactions. Here are the three parameters that we will use to detect whether a transaction is fraudulent or not.

 

1. Upper Control Limit
2. Credit score of each member
3. Zip code distance



