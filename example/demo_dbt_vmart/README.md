Welcome to your dbt project using Vertica's VMart sample database!


### Description of the Solution:
This solution provides an example of a data transformation pipeline using Vertica's sample database VMart. The solution consist of a dbt project that transforms data with the purpose of business analytics and reporting. We want to calculate the on-time delivery rate, quantity accuracy rate and the perfect order rate by vendor. We call the final model vendor_performance. The vendor_performance model is created throughout a series of transformation steps that begin with the raw data. 

For details of how to use this solution, how to configure the environment and how to connect to Vertica using the dbt-vertica adapter, see the solutions guide document in pdf format provided inside this package and also published in the [Vertica community website](https://www.vertica.com/documentation/vertica/technology-partner-integrations/).


### Environment and Versions Tested:
This solution has been developed using the following software and versions: 
- Vertica Server 12.0.3-0 
- Python 3.11
- vertica-python client 1.3.0
- dbt-vertica 1.4.4
- dbt-core 1.4.4
- dbt-tests-adapter 1.4.4


### Resources:
- Learn more about Vertica [in the docs](https://www.vertica.com/docs/12.0.x/HTML/Content/Home.htm)
- Learn more about dbt-vertica adapter [in GitHub](https://github.com/vertica/dbt-vertica)
- Learn more about [Vertica and dbt integration](https://www.vertica.com/documentation/vertica/technology-partner-integrations/)
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Check out [the blo](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
