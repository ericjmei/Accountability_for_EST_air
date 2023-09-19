%%%%%%%%%%%%%%%%%%%%%%
%%% validate PUDL-downloaded data against EPA CAMD CEMS to ensure it's the
%%% same
%%% created: 1/27/2023
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%
%%% inputs
%%%%%%%%%%%%%%%%%%%%%%

fp = fileparts(matlab.desktop.editor.getActiveFilename);
cd(fp) %% change to code folder

cd ../../Data/CAMD % change to data folder

% PUDL
GA_PUDL_2019 = readtable("GA_PUDL_2019_for_validation.csv"); % 2019 hourly dataset, from PUDL

GA_PUDL_2019_asParquet = parquetread("GA_PUDL_2019_for_validation.parquet"); % 2019 hourly dataset, from PUDL

% CAMD
GA_CEMS_2019 = readtable("GA_CEMS_2019_for_validation.csv"); % 2019 hourly dataset, from EPA CEMS

%% 