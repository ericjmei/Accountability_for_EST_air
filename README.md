<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a name="readme-top"></a>
<!--
*** Thanks for checking out the Best-README-Template. If you have a suggestion
*** that would make this better, please fork the repo and create a pull request
*** or simply open an issue with the tag "enhancement".
*** Don't forget to give the project a star!
*** Thanks again! Now go create something AMAZING! :D
-->

<h1 align="center">Accountability Study: Impacts of fuel prices and regulations on electricity generation emissions and urban air quality</h3>

  <p align="center">
    for publication in ES&T Air
    <br />
  </p>
</div>



<!-- ABOUT THE PROJECT -->
## About The Project

These scripts were used in data pre-processing, machine learning model training and use, and plotting for the accompanying publication in ES&T Air:

While these scripts interact with data resulting from the dispatch model (Simple Dispatch found [here](https://github.com/ericjmei/simple_dispatch_total_emissions)), they do not include these scripts. 

Data are available upon request.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- DIRECTORY STRUCTURE -->
## Directory Structure
Scripts in each directory are named in alphabetical order depending on the order they should be run (e.g., "a1\_do\_something", then "a2\_do\_something\_else", then "b\_do\_another\_thing"). Because the dispatch model is part of a separate repository, the model should be run prior to the evaluation step.

- **0. Exploration/** preliminary data exploration
- **1. Pre-processing/** import and formatting of CEMS data, prepartion of pollutant concentration and other data to consistent format
	- **a1\_CEMS\_import.py** uses the public utility data liberation (PUDL) project to retrieve CEMS data from each state for the NERC/BA regions the dispatch model is being run for. Link to PUDL in case the workflow changes.
	- **a2\_CEMS\_tiemzone\_shift\_and\_bin\_daily.py** changes all timezones from the CEMS import to local time (from UTC) and bins data to a daily resolution (by summing).
	- **a3\_CEMS\_from\_SD\_for\_ML.py** perhaps a bit misfiled. This script is intended to be used after the dispatch model is run but before ML training has occured, in which the script "stitches" distinct historical emissions data from multiple model years and puts them on a consistent time basis as the ML training data (to ensure the data have the same missing days). Output from this script can be copy-pasted into the excel file for ML training data.
	- **b\_ATL\_dataPrep.py** and **b\_NYC\_dataPrep.py** format preliminary ML training data by performing recursive feature selection and removing highly correlated features.
- **2. Models/** fitting of ML models
	- **a\_fit\_all\_ML\_models.py** performs ML model training
- **3. Evaluation/** evaluation and processing of dispatch model outputs along with use of trained ML models to obtain counterfactual pollutant concentrations
	- **a1\_eval\_importance+metrics\_all\_ML\_models.py** evaluates the performance of the trained models. Created figure s6.
	- **a2\_ML\_plot\_observed\_modeled\_timeseries.mlx** plots training data with paired predicted data.
	- **a3\_ML\_plot\_observed\_modeled\_corr\_bin\_month.mlx** plots correlation between paired training data and predicted data when binned to monthly resolution (mean).
	- **b1\_resample\_CEMS\_sym\_multiple\_states\_within\_region.py** uses simple dispatch code to calculate historical emissions for given BA regions or NERC regions over specified time. Could be modified to sum emissions among EGUs with any given features as long as these features are in eGRID. 
	- **b2\_copy\_NERC\_dispatch\_to\_subset\_file.py** optional script that renames output files of simple dispatch to be consistent with those used in other scripts. Naming convention of most simple dispatch outputs (ones after this script was developed) should have fixed the naming convention, but this is still here just in case.
	- **b3\_Simple\_Dispatch\_aggregate\_subsets.py** aggregates (sums) simple dispatch outputs from different BA/NERC regions to one region (e.g., for NYC region).
	- **b4\_Simple\_Dispatch\_error.m** calculates error metrics for simple dispatch modeled actual outputs vs. historically observed emissions for different time resolutions (hourly, daily, monthly) for NERC regions. Can be edited for BA regions.
	- **b5\_Simple\_Dispatch\_plot\_observed\_modeled\_and\_corr\_bin\_day.mlx** plots simple dipatch modeled actual vs observed emissions in timeseries and also in scatterplot.
	- **c1\_stitch\_fuel\_price.py** takes fuel price metrics from simple dispatch outputs from different years and puts them together. Note that the simple dispatch fuel price metrics need to be copied from a run of the actual model into the folder that the fuel prices will be retrieved from
	- **c2\_plot\_counterfactual\_fuel\_price.mlx** plots actual vs counterfactual fuel prices. Note that the counterfactual fuel prices need to be specified in a separate excel sheet, in which fuel prices from the specified time periods are averaged.
	- **c3\_plot\_actual\_vs\_counterfactual\_SD.mlx** plots actual vs counterfactual simple dispatch modeled outputs (counterfactual is unadjusted via linear regression at this stage). Created figure s5.
	- **c4\_calc\_short\_cf\_emissions\_monte\_carlo.py** performs linear regression between observed and simple dispatch modeled actual emissions, then uses counterfactual modeled emissions and linear regression to randomly sample 5,000 timeseries of adjusted short-run counterfactual emissions.
	- **c5\_plot\_merit\_order.py** plots merit orders of specified region and week. Created figures s4, s7, and s10.
	- **d1\_create\_total\_cf\_ERs.py** creates ERs based on historically observed emissions for the time periods and regions specified. Can separate ozone and non-ozone seasons for total counterfactual emissions purposes.
	- **d2\_calc\_avg\_ER.py** averages calculated ERs across day of year and reports standard deviation of averages.
	- **d3\_calc\_total\_cf\_emissions\_monte\_carlo.py** samples 5,000 timeseries of total counterfactual emissions using the ERs.
	- **e1\_cf\_emissions\_to\_air\_pollutant.py** pushes timeseries of counterfactual emissions through trained ML models to obtain timeseries of pollutant concentrations
	- **e2\_bin\_monte\_carlo.py** bins the monte carlo timeseries for counterfactual emissions and pollutants to a median, 5th percentile, and 95th percentile.
	- **e3\_plot\_observed\_short\_counterfactual.mlx** plots observed vs. short-run counterfactual emissions and pollutants
	- **e4\_plot\_observed\_all\_counterfactual.mlx** plots observed vs. short-run and total counterfactual emissions and pollutants
	- **e5\_calc\_emissions\_reductions.py** calculates short-run emissions impact (difference between short-run and observed emissions) and medium-to-long-run emissions impact (difference between total and short-run emissions).
	- **e6\_fit\_emissions\_reductions\_with\_ratio.py** fits cubic splines to different periods of medium-to-long-run emissions reductions. 
	- **e7\_extend\_emissions\_reductions.mlx** extends the last fitted value of the cubic splines to the end of the period. 
	- **e8\_propagate\_reductions\_to\_AQ.py** uses uncertainty of emissions reductions to sample 5,000 random emissions impact timeseries. Adds them to observed emissions to put them through ML model to obtain impact of emissions reductions on AQ. 
	- **e9\_calculate\_modeled\_actual\_AQ.py** uses trained ML models and training data to obtain ML-modeled actual pollutant concentrations for baseline to compare with the modeled counterfactual pollutant concentrations.
	- **f1\_join\_AMPD\_eGRID.py** joins air markets program data with eGRID-created generator data objects (from simple dispatch) to allow better interpretation of when controls were installed on certain EGUs
	- **f2\_create\_controls\_timeseries.py** calculates controls installation on annual resolution. 
	- **f3\_process\_EIA\_860\_create\_capacity\_timeseries.py** processes EIA form 860m to calculate capacity of EGU categories added and removed over the period analyzed
	- **f4\_plot\_average\_coal\_ER\_timeseries.py** and **f5\_plot\_average\_ng\_ER\_timeseries.py** calculate average ERs of coal and ng CC EGUs over period. 
- 4. Plots for publication/: all main and supplemental figures used in the publication
	- **fig\_emissions\_reductions\_magnitudes.m** figure 5 a-d
	- **fig\_emission\_reductions\_ratios.m** figure 4
	- **fig\_emissions\_time\_series.mlx** figure 3
	- **fig\_model\_evaluation.mlx** figure 2
	- **fig\_pollutants\_reductions\_magnitudes.m** figure 5 e-h
	- **sup\_fig\_capacity\_time\_series.ml**x figure s8
	- **sup\_fig\_controls\_time\_series.mlx** figure s11
	- **sup\_fig\_emissions\_reductions\_magnitudes.mlx** figure s12
	- **sup\_fig\_ER\_coal\_time\_series.mlx** and **sup\_fig\_ER\_ng\_time\_series.mlx** figure S9
	- **sup\_fig\_intro\_trends.mlx** figures s1-s3
	- **sup\_fig\_pollutants\_reductions\_magnitudes.mlx** figure s14
	- **sup\_fig\_pollutants\_time\_series.mlx** figure s13

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- PACKAGES USED -->
## Packages Used

- XGboost
- sklearn
- dask
- pandas
- xlsxwrite
- openpyxl
- pickle
- pyarrow
- scipy

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Eric Mei - emei3@gatech.edu

Project Link: [https://github.com/ericjmei/Accountability\_for\_EST\_air](https://github.com/ericjmei/Accountability_for_EST_air)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/github_username/repo_name.svg?style=for-the-badge
[contributors-url]: https://github.com/github_username/repo_name/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/github_username/repo_name.svg?style=for-the-badge
[forks-url]: https://github.com/github_username/repo_name/network/members
[stars-shield]: https://img.shields.io/github/stars/github_username/repo_name.svg?style=for-the-badge
[stars-url]: https://github.com/github_username/repo_name/stargazers
[issues-shield]: https://img.shields.io/github/issues/github_username/repo_name.svg?style=for-the-badge
[issues-url]: https://github.com/github_username/repo_name/issues
[license-shield]: https://img.shields.io/github/license/github_username/repo_name.svg?style=for-the-badge
[license-url]: https://github.com/github_username/repo_name/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/linkedin_username
[product-screenshot]: images/screenshot.png
[Next.js]: https://img.shields.io/badge/next.js-000000?style=for-the-badge&logo=nextdotjs&logoColor=white
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 