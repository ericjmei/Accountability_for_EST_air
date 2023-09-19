%%%%%%%%%%%%%%%%%%%%%%
%%% calculate RMSE, NME, NMB, R^2, and other metrics on Simple Dispatch emissions data
%   on hourly, daily, and monthly resolutions for each year for each NERC
%   region
%%% created: 3/27/2023
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%
%%% inputs
%%%%%%%%%%%%%%%%%%%%%%

rel_path_actual_dispatch = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS";
rel_path_simple_dispatch = "../../Data/Simple Dispatch Outputs/2023-06-23 act";
rel_path_output = "../../Data/Simple Dispatch Outputs/2023-06-23 act";
% for ba regions
filename_ending = {["SOCO"], ["TVA"], ["PJM"], ["NYIS"], ["ISNE"], ["NYC"], ["SE"]};
year = 2006:2019; % input one or a range of years (consecutive)

fp = fileparts(matlab.desktop.editor.getActiveFilename);
cd(fp) %% change to code folder

%%%%%%%%%%%%%%%%%%%%%%
%%% calculate error metrics for all regions desired
%%%%%%%%%%%%%%%%%%%%%%

for j = 1:size(filename_ending, 2) % loop through all subsetted regions

    % retrieve data from the states necessary
    obsDataAll = retrieve_data(year(1), filename_ending{j}, rel_path_actual_dispatch);
    mdlDataAll = retrieve_data(year(1), filename_ending{j}, rel_path_simple_dispatch);
    if length(year)~=1 % if range of years,
        for i = 2:length(year) % loop through remaining years and append to one large loop
            % read in remaining data
            tempObs = retrieve_data(year(i), filename_ending{j}, rel_path_actual_dispatch);
            tempMdl = retrieve_data(year(i), filename_ending{j}, rel_path_simple_dispatch);

            % append to larger dataset
            obsDataAll = [obsDataAll; tempObs];
            mdlDataAll = [mdlDataAll; tempMdl];
        end
    end
    %%% clean data 
    % bin data by day
    % observed data
    obsDataAll = table2timetable(obsDataAll); % convert to timetable
    obsDataAll = obsDataAll(:, ["so2_tot", "nox_tot"]); % keep only emissions columns
    obsDataAll = retime(obsDataAll, 'daily', 'sum'); % change obs to daily
    time = obsDataAll.Properties.RowTimes; % copy times
    obsDataAll = obsDataAll(obsDataAll.Properties.RowTimes<datetime(2020, 1, 1), :); % keep only pre-2020 data
    obsDataAll = timetable2table(obsDataAll); % turn timetable back into table
    % modeled data
    mdlDataAll = table2timetable(mdlDataAll); % convert to timetable
    mdlDataAll = mdlDataAll(:, ["so2_tot", "nox_tot"]); % keep only emissions columns
    mdlDataAll = retime(mdlDataAll, 'daily', 'sum'); % change mdl to daily
    mdlDataAll = mdlDataAll(mdlDataAll.Properties.RowTimes<datetime(2020, 1, 1), :); % keep only pre-2020 data
    mdlDataAll = timetable2table(mdlDataAll); % turn data into table

    % create filler tables for each emissions species
    template = cell2table(cell(2,5), 'VariableNames', {'Resolution', 'R2', 'RMSE', 'NME', 'NMB'});
    metrics = struct(); % structure for holding each species's table
    species = ["so2", "nox"]; % all species to calculate
    metrics.so2 = template;
    metrics.nox = template;

    %%% calculate daily resolution metrics
    for i = 1:length(species) % loop through all species
        % retrieve emissions data
        obsData = obsDataAll.(strcat(species(i), "_tot"));
        mdlData = mdlDataAll.(strcat(species(i), "_tot"));

        [R2, RMSE, NME, NMB] = calcMetrics(obsData, mdlData); % calculate metrics

        % put into table
        toTable = {'Daily', R2, RMSE, NME, NMB};
        metrics.(species(i))(2, :) = array2table(toTable);
    end

    %%% calculate monthly resolution metrics
    % bin data by month
    inds = time >= datetime(year(1), 1, 1) & time < datetime(year(end)+1, 1, 1); % time bounds
    % observed data
    obsDataAllAveraged = retime(table2timetable(obsDataAll(inds, :)), 'monthly', 'sum'); % change obs to monthly
    obsDataAllAveraged = timetable2table(obsDataAllAveraged); % turn data into table
    % modeled data
    mdlDataAllAveraged = retime(table2timetable(mdlDataAll(inds, :)), 'monthly', 'sum'); % change mdl to monthly
    mdlDataAllAveraged = timetable2table(mdlDataAllAveraged); % turn data into table

    % calculate metrics
    for i = 1:length(species) % loop through all species
        % retrieve emissions data
        obsData = obsDataAllAveraged.(strcat(species(i), "_tot"));
        mdlData = mdlDataAllAveraged.(strcat(species(i), "_tot"));

        [R2, RMSE, NME, NMB] = calcMetrics(obsData, mdlData); % calculate metrics

        % put into table
        toTable = {'Monthly', R2, RMSE, NME, NMB};
        metrics.(species(i))(3, :) = array2table(toTable);
    end

    %%% write to table, each pollutant gets one sheet
    % base naming convention for file names
    if length(year)==1 % if one year, only include that year
        fn = strcat(strjoin(filename_ending{j}, '_'), '_', num2str(year));
    else % if multiple years, include first and last year
        fn = strcat(strjoin(filename_ending{j}, '_'), '_', num2str(year(1)), ...
            "-", num2str(year(end)));
    end

    % write in data
    cd(fp)
    cd(rel_path_output)
    for i = 1:length(species)
        writetable(metrics.(species(i)), ...
            strcat("hist_eval_metrics_", fn, ".xlsx"), "Sheet", species(i))
    end

end

function [R2, RMSE, NME, NMB] = calcMetrics(obsData, mdlData)
%%% calculate R2, RMSE, NME, and NMB for observed and calculated (modeled) data

R2 = corr(obsData, mdlData, "rows", "complete"); % R^2
RMSE = rootMeanSD(obsData, mdlData); % RMSE
NME = normMeanError(mdlData, obsData); % NME
NMB = normMeanBias(mdlData, obsData); % NMB, order matters; if positive, calc > obs
end

function [data] = retrieve_data(year, group_of_states, rel_file_path)
fp = fileparts(matlab.desktop.editor.getActiveFilename);
cd(fp) %% change to code folder
cd(rel_file_path) % change to relative file path
fn_end = strcat(strjoin(group_of_states, '_'), '_', num2str(year), '.csv');
fn = retrieve_filename(fn_end);
data = readtable(fn{1});
end

function [fn] = retrieve_filename(fn_end)
% retrieve all file names
all_files = dir;
all_files = {all_files.name}; 
indices = find(endsWith(all_files, fn_end)); % find matching end of file name
fn = all_files(indices); % retrieve file name
end