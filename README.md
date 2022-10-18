# SIMS DATA PIPELINE

SIMS (SITE Improvement through Monitoring Systems) is a PEPFAR guided tool that aims to assess the performance of Facilities in order to meet the programme’s standards. The tool contains 2 forms, Above Site and Site. Currently, the tool in use is the Site Tool, where individual facilities are gauged based on the reviews done on them, typically on a quarterly basis. The form contains 16 ‘sets’ of different forms, each with sections known as Core Essential Elements (cees) that determine the score of a facility. SGHI developed the tool using Xforms on ODK.

For further reference, kindly refer to the documentation: [SIMS FY21](https://www.state.gov/wp-content/uploads/2020/10/FY21-SIMS-4.1-Implementation-Guide_15Aug2020.pdf)

## Description

Site Assessment tool contains 16 sets of forms with a total of 140 cees that are aggregated from over 1,300 Data Elements. These Data Elements each have detailed questions and/or labels that are displayed as encoded titles (IDs) on the submissions output. The process aims to find an efficient way to match the encoded titles to the question or label associated to it - in a manner that enables appropriate visualizations.

Due to the nature of the detailed questions and/or labels, the representation of the output needs to be referenced to a lookup table. This lookup table contains the IDs of all the Data Elements in one column and their respective question and/or label in another column. It is a static table that is relevant to SIMS version 4.1, and will be hosted on FYJ's SIMS BigQuery Data Warehouse.

Once the submissions have been extracted from ODK (daily), they will be transformed to enrich the data with the Facility Name. They will also be enriched with text on responses where the user had to choose between multiple choices (which are encoded as well). Each set's submissions will then be transmitted to Google Cloud Storage and finally to BigQuery Data Warehouse.

Finally, there is a secondary static lookup table, with MFL Codes and organizational unit heirachy details, that will be used to further enrich the data.

For visual representation visit: [SIMS layout](https://wiki.fahariyajamii.org/en/development/ODK_Pipeline)

## Usage

### submissions

The pipeline enforces data loading from ODK Central to GCS bucket to BigQuery. Within the workflow there are tasks that enrich the data to make it more readable and accessible for visualization. Each form is loaded into it's own BigQuery table that can be accessed from the Data WareHouse. It is paramount to understand the naming convention as each form known as a set, has its own group of subsections (cees) that contribute to the average score for each facility that has a submission.

### transforms

This pipeline entails additional transformations from which Hub details are added onto the data. Hubs are regions from which the programme operates and are different from county and subcounty details. A Hub could run over multiple subcounties.

### Workflow

Scheduled to run at 3.00 am EAT, the DAG contained on *sims_subs* runs first. Upon it's successful completion, it triggers the  *sims_transforms* DAG through a **TriggerDagRunOperator** on Airflow. The intended purpose is to have the loading and transfortaion from the first DAG completed, before executing the transformations DAG.

For each form/set/table, the code has descriptive DAG task ID's to explain the transformation done by each task.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)