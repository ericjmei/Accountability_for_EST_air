%%%%%%%%%%%%%%%%%%%%%%
%%% plot all pollutant reductions from:
% 1. short-run fuel price impacts
% 2. CAIR impacts
% 3. CSAPR, MATS, and long-run fuel price impacts
% 4. total counterfactual impacts
% importantly, the reductions are between the modeled actual and modeled counterfactual
% epi studies use obs - (act - cf). We are doing this to stay consistent with them
% uncertainty is entirely from the counterfactual, however, since the fixed portion is in the actual
%%% REMEMBER TO CLEAR THE IN-LINE OUTPUT AFTER PLOTTING
%%% created: 6/02/2023
%%%%%%%%%%%%%%%%%%%%%%

%% file paths
fp = fileparts(matlab.desktop.editor.getActiveFilename);
% directory with counterfactual air pollutants
rel_path_input_pollutants_short_cf = "../../Data/Counterfactual Air Pollutants/7. ba regions edited";
rel_path_input_pollutants_total_cf = "../../Data/Counterfactual Air Pollutants/8. total cf edited";
rel_path_input_pollutants_impact = "../../Data/Counterfactual Air Pollutants/7. ba regions edited";
% directory with observed emissions and pollutants
rel_path_observed = "../../Data/Analysis";
% directory with modeled actual pollutants
rel_path_actual = "../../Data/Counterfactual Air Pollutants/0. modeled actual";

% directory to plot outputs
rel_path_output = "../../Figures/for publication";

%% miscellaneous inputs for retrieving data
% naming conventions are all different, unfortunately
years_all = 2006:2019; % input one or a range of years (consecutive)
regions_emissions_all = ["ATL", "NYC"]; % for storing
fn_ends = {["SOCO"], ["NYC"]};
% species to plot for ML (so that we can increase the number of species at
% a later time
target_names_all = ["pm25", "ozone"];
% label units to map to (must be parallel to target names)
label_units_all = ["(\mug/m^3)", "(ppb)"];
labelDictionary = containers.Map(target_names_all, label_units_all);
% sites to plot for ML
sites_ML_all = ["SDK", "Bronx", "Manhattan", "Queens"];

%%%%%%%%%%%%%%%%%%%%%%
%%% import and process data
%%%%%%%%%%%%%%%%%%%%%%

%%% pre-allocate things to plot
pollutants_total_cf = struct();
pollutants_short_cf = struct();
pollutants_CAIR_cf = struct();
pollutants_other_cf = struct();
pollutants_total_diff = struct();
pollutants_short_diff = struct();
pollutants_CAIR_diff = struct();
pollutants_other_diff = struct();
pollutants_total_cf_unbinned = struct();
pollutants_total_diff_unbinned = struct();
pollutants_short_diff_unbinned = struct();
pollutants_CAIR_diff_unbinned = struct();
pollutants_other_diff_unbinned = struct();
pollutants_obs_unbinned = struct();
pollutants_obs = struct();
pollutants_act = struct(); % modeled actual

%%% establish time spacing
spacing = datetime(years_all(1), 1, 1):calmonths(1):datetime(years_all(end)+1, 1, 1);
spacing = [spacing(1:end-1)', spacing(2:end)'-caldays(1)]; % start and end of bins
spacing = datenum(spacing); % to datenum for binning function

%%% loop through ML sites, import, process, and store all data
for i = 1:length(sites_ML_all)
    site = sites_ML_all(i);

    % loop through species
    for j = 1:length(target_names_all)
        target = target_names_all(j);

        % retrieve observed data
        cd(fp)
        cd(strcat(rel_path_observed, "/", site))
        pollutant_obs = readtable(...
            strcat(site, "_observed_modeled_timeseries_XGB.xlsx"),...
            "Sheet", strcat("y_", target), "VariableNamingRule", "preserve");
        % retrieve counterfactual data
        cd(fp)
        cd(rel_path_input_pollutants_total_cf)
        pollutant_total_cf = parquetread(strcat(site, "_", target, "_", num2str(years_all(1)),...
            "-", num2str(years_all(end)), "_bin_daily.parquet"));
        cd(fp)
        cd(rel_path_input_pollutants_short_cf)
        pollutant_short_cf = parquetread(strcat(site, "_", target, "_", num2str(years_all(1)),...
            "-", num2str(years_all(end)), "_bin_daily.parquet"));
        % retrieve impact data
        cd(fp)
        cd(rel_path_input_pollutants_impact)
        pollutant_CAIR_cf = parquetread(strcat(site, "_", target, "_", num2str(years_all(1)),...
            "-", num2str(years_all(end)), "_CAIR_bin_daily.parquet"));
        pollutant_other_cf = parquetread(strcat(site, "_", target, "_", num2str(years_all(1)),...
            "-", num2str(years_all(end)), "_other_bin_daily.parquet"));
        % retrieve modeled actual data
        cd(fp)
        cd(rel_path_actual)
        pollutant_act = parquetread(strcat(site, "_", target, "_", num2str(years_all(1)),...
            "-", num2str(years_all(end)), "_bin_daily.parquet"));

        % remove days where there is no observed data
        % observed
        inds = isnan(pollutant_obs.observed); % nan observations
        pollutant_obs(inds, :) = [];
        % counterfactual
        inds = ~ismember(pollutant_total_cf.Date, pollutant_obs.Date);
        pollutant_total_cf(inds, :) = [];
        inds = ~ismember(pollutant_short_cf.Date, pollutant_obs.Date);
        pollutant_short_cf(inds, :) = [];
        % impacts
        inds = ~ismember(pollutant_CAIR_cf.Date, pollutant_obs.Date);
        pollutant_CAIR_cf(inds, :) = [];
        inds = ~ismember(pollutant_other_cf.Date, pollutant_obs.Date);
        pollutant_other_cf(inds, :) = [];
        % actual
        inds = ~ismember(pollutant_act.Date, pollutant_obs.Date);
        pollutant_act(inds, :) = [];
        
        % calculate differences and multiply differences by -1 to obtain reductions
        pollutant_total_diff = subtractTableAndMakeNegative(pollutant_total_cf, pollutant_act.median);
        pollutant_short_diff = subtractTableAndMakeNegative(pollutant_short_cf, pollutant_act.median);
        pollutant_CAIR_diff = subtractTableAndMakeNegative(pollutant_CAIR_cf, pollutant_act.median);
        pollutant_other_diff = subtractTableAndMakeNegative(pollutant_other_cf, pollutant_act.median);
        
        % store in unbinned structure
        pollutants_total_cf_unbinned.(strcat(site, "_", target)) = pollutant_total_cf;
        pollutants_total_diff_unbinned.(strcat(site, "_", target)) = pollutant_total_diff;
        pollutants_short_diff_unbinned.(strcat(site, "_", target)) = pollutant_short_diff;
        pollutants_CAIR_diff_unbinned.(strcat(site, "_", target)) = pollutant_CAIR_diff;
        pollutants_other_diff_unbinned.(strcat(site, "_", target)) = pollutant_other_diff;
        pollutants_obs_unbinned.(strcat(site, "_", target)) = pollutant_obs;

        % bin by month
        % observed
        [pollutant_obs, ~, ~, ~] = BinAvg(datenum(pollutant_obs.Date), pollutant_obs.observed, ...
            spacing, 15, 3);
        % counterfactual
        pollutant_total_cf = binCounterfactual(pollutant_total_cf, spacing);
        pollutant_short_cf = binCounterfactual(pollutant_short_cf, spacing);
        pollutant_total_diff = binCounterfactual(pollutant_total_diff, spacing);
        pollutant_short_diff = binCounterfactual(pollutant_short_diff, spacing);
        % impacts
        pollutant_CAIR_cf = binCounterfactual(pollutant_CAIR_cf, spacing);
        pollutant_other_cf = binCounterfactual(pollutant_other_cf, spacing);
        pollutant_CAIR_diff = binCounterfactual(pollutant_CAIR_diff, spacing);
        pollutant_other_diff = binCounterfactual(pollutant_other_diff, spacing);
        % actual
        pollutant_act = binCounterfactual(pollutant_act, spacing);

        % store in structure
        pollutants_total_cf.(strcat(site, "_", target)) = pollutant_total_cf;
        pollutants_short_cf.(strcat(site, "_", target)) = pollutant_short_cf;
        pollutants_CAIR_cf.(strcat(site, "_", target)) = pollutant_CAIR_cf;
        pollutants_other_cf.(strcat(site, "_", target)) = pollutant_other_cf;
        pollutants_total_diff.(strcat(site, "_", target)) = pollutant_total_diff;
        pollutants_short_diff.(strcat(site, "_", target)) = pollutant_short_diff;
        pollutants_CAIR_diff.(strcat(site, "_", target)) = pollutant_CAIR_diff;
        pollutants_other_diff.(strcat(site, "_", target)) = pollutant_other_diff;
        pollutants_obs.(strcat(site, "_", target)) = pollutant_obs;
        pollutants_act.(strcat(site, "_", target)) = pollutant_act;
    end
end

%%%%%%%%%%%%%%%%%%%%%%
%%% plot
%%% we want a 2 x 2 plot, so we'll make 2 columns
%%% 1. ATL site
%%% 2. NYC site
%%% make this vertical
%%%%%%%%%%%%%%%%%%%%%%

%% figure attributes
colors = [0, 114, 178;  % blue, short-run
          230, 159, 0; % yellow, CAIR
          110, 110, 110;  % gray, other
          213, 94, 0;
          213, 94, 0]/255; % unapportioned
colors_transparency = [0.25, 0.25, 0.25, 0.25]; % parallel to colors being used
non_significant_scale = 3/5; % scale for portions of the figure that are not significant
xlims = datetime([2006, 2020], 1, 1);
xtickss = datetime(2006, 1, 1):calyears(2):datetime(2020, 1, 1);
fontSize = 30;
lineWidth = 4;
aspect_ratio = [2.5, 1, 1];

%% 1. PM2.5
fig_pm25 = figure('Position', get(0, 'Screensize')); %for saving the figure
tiledlayout(1, 2, "TileSpacing", "compact");
% a. ATL
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total counterfactual
p_total = plot(datenum(pollutants_total_diff.SDK_pm25.Date), pollutants_total_diff.SDK_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(pollutants_total_diff.SDK_pm25.Date), nan_non_significant(pollutants_total_diff.SDK_pm25), ...
    'LineWidth', lineWidth, 'color', colors(4, :));
% short-run counterfactual
p_short = plot(datenum(pollutants_short_diff.SDK_pm25.Date), pollutants_short_diff.SDK_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
p_short.Color(4) = colors_transparency(1);
plot(datenum(pollutants_short_diff.SDK_pm25.Date), nan_non_significant(pollutants_short_diff.SDK_pm25), ...
    'LineWidth', lineWidth, 'color', colors(1, :));
% CAIR
p_CAIR = plot(datenum(pollutants_CAIR_diff.SDK_pm25.Date), pollutants_CAIR_diff.SDK_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(pollutants_CAIR_diff.SDK_pm25.Date), nan_non_significant(pollutants_CAIR_diff.SDK_pm25), ...
    'LineWidth', lineWidth, 'color', colors(2, :));
% CSAPR, MATS, and other
p_other = plot(datenum(pollutants_other_diff.SDK_pm25.Date), pollutants_other_diff.SDK_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(pollutants_other_diff.SDK_pm25.Date), nan_non_significant(pollutants_other_diff.SDK_pm25), ...
    'LineWidth', lineWidth, 'color', colors(3, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-8, 1])
yticks(-8:4:0)

% labels
ax.FontSize = fontSize;
ylabel({'\DeltaPM_{2.5}'; '(\mug/m^3)'})
xlabel("Year")

hold off

% b. nyc (Queens)
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total counterfactual
p_total = plot(datenum(pollutants_total_diff.Queens_pm25.Date), pollutants_total_diff.Queens_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(pollutants_total_diff.Queens_pm25.Date), nan_non_significant(pollutants_total_diff.Queens_pm25), ...
    'LineWidth', lineWidth, 'color', colors(4, :));
% short-run counterfactual
p_short = plot(datenum(pollutants_short_diff.Queens_pm25.Date), pollutants_short_diff.Queens_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
p_short.Color(4) = colors_transparency(1);
plot(datenum(pollutants_short_diff.Queens_pm25.Date), nan_non_significant(pollutants_short_diff.Queens_pm25), ...
    'LineWidth', lineWidth, 'color', colors(1, :));
% CAIR
p_CAIR = plot(datenum(pollutants_CAIR_diff.Queens_pm25.Date), pollutants_CAIR_diff.Queens_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(pollutants_CAIR_diff.Queens_pm25.Date), nan_non_significant(pollutants_CAIR_diff.Queens_pm25), ...
    'LineWidth', lineWidth, 'color', colors(2, :));
% CSAPR, MATS, and other
p_other = plot(datenum(pollutants_other_diff.Queens_pm25.Date), pollutants_other_diff.Queens_pm25.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(pollutants_other_diff.Queens_pm25.Date), nan_non_significant(pollutants_other_diff.Queens_pm25), ...
    'LineWidth', lineWidth, 'color', colors(3, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-10, 1])

% labels
ax.FontSize = fontSize;
xlabel("Year")

hold off

%% 2. ozone
fig_ozone = figure('Position', get(0, 'Screensize')); %for saving the figure
tiledlayout(1, 2, "TileSpacing", "compact");

% a. ATL
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total counterfactual
p_total = plot(datenum(pollutants_total_diff.SDK_ozone.Date), pollutants_total_diff.SDK_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(pollutants_total_diff.SDK_ozone.Date), nan_non_significant(pollutants_total_diff.SDK_ozone), ...
    'LineWidth', lineWidth, 'color', colors(4, :));
% short-run counterfactual
p_short = plot(datenum(pollutants_short_diff.SDK_ozone.Date), pollutants_short_diff.SDK_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
p_short.Color(4) = colors_transparency(1);
plot(datenum(pollutants_short_diff.SDK_ozone.Date), nan_non_significant(pollutants_short_diff.SDK_ozone), ...
    'LineWidth', lineWidth, 'color', colors(1, :));
% CAIR
p_CAIR = plot(datenum(pollutants_CAIR_diff.SDK_ozone.Date), pollutants_CAIR_diff.SDK_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(pollutants_CAIR_diff.SDK_ozone.Date), nan_non_significant(pollutants_CAIR_diff.SDK_ozone), ...
    'LineWidth', lineWidth, 'color', colors(2, :));
% CSAPR, MATS, and other
p_other = plot(datenum(pollutants_other_diff.SDK_ozone.Date), pollutants_other_diff.SDK_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(pollutants_other_diff.SDK_ozone.Date), nan_non_significant(pollutants_other_diff.SDK_ozone), ...
    'LineWidth', lineWidth, 'color', colors(3, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-6, 2])

% labels
ax.FontSize = fontSize;
xlabel("Year")
ylabel({'\DeltaOzone'; '(ppb)'})

hold off

% b. nyc
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total counterfactual
p_total = plot(datenum(pollutants_total_diff.Queens_ozone.Date), pollutants_total_diff.Queens_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(pollutants_total_diff.Queens_ozone.Date), nan_non_significant(pollutants_total_diff.Queens_ozone), ...
    'LineWidth', lineWidth, 'color', colors(4, :));
% short-run counterfactual
p_short = plot(datenum(pollutants_short_diff.Queens_ozone.Date), pollutants_short_diff.Queens_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
p_short.Color(4) = colors_transparency(1);
plot(datenum(pollutants_short_diff.Queens_ozone.Date), nan_non_significant(pollutants_short_diff.Queens_ozone), ...
    'LineWidth', lineWidth, 'color', colors(1, :));
% CAIR
p_CAIR = plot(datenum(pollutants_CAIR_diff.Queens_ozone.Date), pollutants_CAIR_diff.Queens_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(pollutants_CAIR_diff.Queens_ozone.Date), nan_non_significant(pollutants_CAIR_diff.Queens_ozone), ...
    'LineWidth', lineWidth, 'color', colors(2, :));
% CSAPR, MATS, and other
p_other = plot(datenum(pollutants_other_diff.Queens_ozone.Date), pollutants_other_diff.Queens_ozone.median, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(pollutants_other_diff.Queens_ozone.Date), nan_non_significant(pollutants_other_diff.Queens_ozone), ...
    'LineWidth', lineWidth, 'color', colors(3, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-2, 6])

% labels
ax.FontSize = fontSize;
xlabel("Year")
% ylabel({'\DeltaOzone'; '(ppb)'})

hold off

%%%%%%%%%%%%%%%%%%%%%%
%%% export
%%%%%%%%%%%%%%%%%%%%%%
%%
fn_beg = "fig_pollutants_reductions";
cd(fp)
cd(rel_path_output)
exportgraphics(fig_pm25, strcat(fn_beg, "_pm25.png"));
exportgraphics(fig_ozone, strcat(fn_beg, "_ozone.png"));

%%%%%%%%%%%%%%%%%%%%%%
%%% additional calculations for publication
%%%%%%%%%%%%%%%%%%%%%%

%% average reductions and percentage reductions (reductions/(-1*(total reductions)+obs)) by 2019
year_to_calc = 2019;
% pre-allocate structures
pm25_diff = struct();
pm25_diff_fraction = struct();
ozone_diff = struct();
ozone_diff_fraction = struct();
pm25_diff_upper_bound = struct();
ozone_diff_upper_bound = struct();
pm25_diff_lower_bound = struct();
ozone_diff_lower_bound = struct();

% loop through and calculate reductions
for i=1:length(sites_ML_all)
    site = sites_ML_all(i);
    
    % total
    [pm25_diff.(strcat("total_", site)), pm25_diff_upper_bound.(strcat("total_", site)), ...
        pm25_diff_lower_bound.(strcat("total_", site)), ...
    ozone_diff.(strcat("total_", site, "_ozone_season")), ozone_diff_upper_bound.(strcat("total_", site, "_ozone_season")),...
    ozone_diff_lower_bound.(strcat("total_", site, "_ozone_season")), ...
    ozone_diff.(strcat("total_", site, "_non_ozone_season")), ozone_diff_upper_bound.(strcat("total_", site, "_non_ozone_season")),...
    ozone_diff_lower_bound.(strcat("total_", site, "_non_ozone_season"))] ...
    = calculate_average_reductions(pollutants_total_diff_unbinned, site, year_to_calc);
    
    [pm25_diff_fraction.(strcat("total_", site)), ozone_diff_fraction.(strcat("total_", site, "_ozone_season")),...
        ozone_diff_fraction.(strcat("total_", site, "_non_ozone_season"))] = ...
    calculate_average_percent_reductions(pollutants_total_diff_unbinned, pollutants_total_cf_unbinned, ...
    site, year_to_calc);

    % short-run
    [pm25_diff.(strcat("short_", site)), pm25_diff_upper_bound.(strcat("short_", site)), ...
        pm25_diff_lower_bound.(strcat("short_", site)), ...
    ozone_diff.(strcat("short_", site, "_ozone_season")), ozone_diff_upper_bound.(strcat("short_", site, "_ozone_season")),...
    ozone_diff_lower_bound.(strcat("short_", site, "_ozone_season")), ...
    ozone_diff.(strcat("short_", site, "_non_ozone_season")), ozone_diff_upper_bound.(strcat("short_", site, "_non_ozone_season")),...
    ozone_diff_lower_bound.(strcat("short_", site, "_non_ozone_season"))] ...
    = calculate_average_reductions(pollutants_short_diff_unbinned, site, year_to_calc);

    [pm25_diff_fraction.(strcat("short_", site)), ozone_diff_fraction.(strcat("short_", site, "_ozone_season")),...
            ozone_diff_fraction.(strcat("short_", site, "_non_ozone_season"))] = ...
        calculate_average_percent_reductions(pollutants_short_diff_unbinned, pollutants_total_cf_unbinned, ...
        site, year_to_calc);

    % CAIR
    [pm25_diff.(strcat("CAIR_", site)), pm25_diff_upper_bound.(strcat("CAIR_", site)), ...
        pm25_diff_lower_bound.(strcat("CAIR_", site)), ...
    ozone_diff.(strcat("CAIR_", site, "_ozone_season")), ozone_diff_upper_bound.(strcat("CAIR_", site, "_ozone_season")),...
    ozone_diff_lower_bound.(strcat("CAIR_", site, "_ozone_season")), ...
    ozone_diff.(strcat("CAIR_", site, "_non_ozone_season")), ozone_diff_upper_bound.(strcat("CAIR_", site, "_non_ozone_season")),...
    ozone_diff_lower_bound.(strcat("CAIR_", site, "_non_ozone_season"))] ...
    = calculate_average_reductions(pollutants_CAIR_diff_unbinned, site, year_to_calc);
    
    [pm25_diff_fraction.(strcat("CAIR_", site)), ozone_diff_fraction.(strcat("CAIR_", site, "_ozone_season")),...
        ozone_diff_fraction.(strcat("CAIR_", site, "_non_ozone_season"))] = ...
    calculate_average_percent_reductions(pollutants_CAIR_diff_unbinned, pollutants_total_cf_unbinned, ...
    site, year_to_calc);

    % other
    [pm25_diff.(strcat("other_", site)), pm25_diff_upper_bound.(strcat("other_", site)), ...
        pm25_diff_lower_bound.(strcat("other_", site)), ...
    ozone_diff.(strcat("other_", site, "_ozone_season")), ozone_diff_upper_bound.(strcat("other_", site, "_ozone_season")),...
    ozone_diff_lower_bound.(strcat("other_", site, "_ozone_season")), ...
    ozone_diff.(strcat("other_", site, "_non_ozone_season")), ozone_diff_upper_bound.(strcat("other_", site, "_non_ozone_season")),...
    ozone_diff_lower_bound.(strcat("other_", site, "_non_ozone_season"))] ...
    = calculate_average_reductions(pollutants_other_diff_unbinned, site, year_to_calc);

    [pm25_diff_fraction.(strcat("other_", site)), ozone_diff_fraction.(strcat("other_", site, "_ozone_season")),...
        ozone_diff_fraction.(strcat("other_", site, "_non_ozone_season"))] = ...
    calculate_average_percent_reductions(pollutants_other_diff_unbinned, pollutants_total_cf_unbinned, ...
    site, year_to_calc);
end

function [output] = binCounterfactual(input, spacing)
% input is input table that has Date, lower_bound, upper_bound, and median
% spacing is bin edges in datenum

% median
[median, ~, ~, timeSpacing] = BinAvg(datenum(input.Date), input.median, ...
    spacing, 15, 3);
% lower bound
[lower_bound, ~, ~, ~] = BinAvg(datenum(input.Date), input.lower_bound, ...
    spacing, 15, 3);
% upper bound
[upper_bound, ~, ~, ~] = BinAvg(datenum(input.Date), input.upper_bound, ...
    spacing, 15, 3);

output = table(timeSpacing, median, lower_bound, upper_bound);
output.Properties.VariableNames = ["Date", "median", "lower_bound", "upper_bound"];

end

function [pm25, pm25_upper_bound, pm25_lower_bound, ...
    ozone_ozone_season, ozone_upper_bound_ozone_season, ozone_lower_bound_ozone_season, ...
    ozone_non_ozone_season, ozone_upper_bound_non_ozone_season, ozone_lower_bound_non_ozone_season] ...
    = calculate_average_reductions(data_reduction, site, year_to_calc)
% calculates average pm25 and ozone reductions for given site data and year to calculate
% returns number for median, upper, and lower bounds
% calculate pm25
pm25 = average_year( ...
    data_reduction.(strcat(site, "_pm25")).median, ...
    data_reduction.(strcat(site, "_pm25")).Date, year_to_calc);
pm25_upper_bound = average_year( ...
    data_reduction.(strcat(site, "_pm25")).upper_bound, ...
    data_reduction.(strcat(site, "_pm25")).Date, year_to_calc);
pm25_lower_bound = average_year( ...
    data_reduction.(strcat(site, "_pm25")).lower_bound, ...
    data_reduction.(strcat(site, "_pm25")).Date, year_to_calc);
% calculate ozone
[ozone_ozone_season, ...
    ozone_non_ozone_season] = average_year_ozone( ...
    data_reduction.(strcat(site, "_ozone")).median, ...
    data_reduction.(strcat(site, "_ozone")).Date, year_to_calc);
[ozone_upper_bound_ozone_season, ...
    ozone_upper_bound_non_ozone_season] = average_year_ozone( ...
    data_reduction.(strcat(site, "_ozone")).upper_bound, ...
    data_reduction.(strcat(site, "_ozone")).Date, year_to_calc);
[ozone_lower_bound_ozone_season, ...
    ozone_lower_bound_non_ozone_season] = average_year_ozone( ...
    data_reduction.(strcat(site, "_ozone")).lower_bound, ...
    data_reduction.(strcat(site, "_ozone")).Date, year_to_calc);
end

function [pm25, ozone_ozone_season, ozone_non_ozone_season] = ...
    calculate_average_percent_reductions(data_reduction_factor, data_total_cf, site, year_to_calc)
% calculates average pm25 and ozone reductions for given site data for a given factor (short-run, total, etc.), 
% modeled total cf data, and year to calculate.
% returns percentage for median only
% percentage = reduction/(modeled total cf)
% calculate pm
pm25 = average_year( ...
    data_reduction_factor.(strcat(site, "_pm25")).median./ ...
    data_total_cf.(strcat(site, "_pm25")).median, ...
    data_reduction_factor.(strcat(site, "_pm25")).Date, year_to_calc);
% calculate ozone
[ozone_ozone_season, ...
    ozone_non_ozone_season] = average_year_ozone( ...
    data_reduction_factor.(strcat(site, "_ozone")).median./ ...
    data_total_cf.(strcat(site, "_ozone")).median, ...
    data_reduction_factor.(strcat(site, "_ozone")).Date, year_to_calc);
end

function [output] = subtractTableAndMakeNegative(input, to_subtract)
% subtracts constant value from all others in a table (with median,
% lower_bound, and upper_bound
output = input;
output.median = -1*(input.median - to_subtract);
% flips lower and upper bounds, but is more accurate to making the
% reductions negative
output.lower_bound = -1*(input.upper_bound - to_subtract);
output.upper_bound = -1*(input.lower_bound - to_subtract);
end

function [output] = average_year(input_values, input_dates, year_to_calc)
% averages input_values for year_to_calc using input_dates
mask = isbetween(input_dates, datetime(year_to_calc, 1, 1), datetime(year_to_calc, 12, 31));
output = mean(input_values(mask));
end

function [output_ozone_season, output_non_ozone_season] = average_year_ozone(input_values, input_dates, year_to_calc)
% averages median of input_table for year_to_calc
% mask for ozone season
mask = isbetween(input_dates, datetime(year_to_calc, 5, 1), datetime(year_to_calc, 9, 30));
output_ozone_season = mean(input_values(mask));
output_non_ozone_season = mean(input_values(~mask));
end

function [output] = nan_non_significant(input)
% returns vector of median from 'input' table in which non-significant
% values are marked nan
% 'input' table MUST have lower_bound, upper_bound, and median

% mask for values where upper and lower bound have opposite signs (include
% 0, in which they are not significant)
mask = mask_opposite_sign(input.lower_bound, input.upper_bound);

% set those values to nan
output = input.median;
output(mask) = nan;
end

function [output] = mask_opposite_sign(col1, col2)
% returns vector same length as col1 or col2 of a table (insert whole
% column as vector)
% returns 'true' for rows with opposite sign

% Check if 'col1' and 'col2' have opposite signs for each row
output = (col1 < 0 & col2 >= 0) | (col1 >= 0 & col2 < 0);
end