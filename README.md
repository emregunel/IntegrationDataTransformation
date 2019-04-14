# Daily Data Wrangler

This repository demonstrates the work I had done for one of my clients

Used technologies: Python, Pandas, AWS, Lambda, CloudWatch, Salesforce

## Problem: 

Integration need to be built between internal system and Salesforce. 

Data source was not able to give .csv as Salesforce can handle.

## Solution: 

Built a lambda Python(with Pandas library) function on Amazon Web Services to transform the data.

Function is running on AWS with a cloud watch event that triggers every day at given hour.
