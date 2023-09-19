%%%%%%%%%%%%%%%%%%%%%%
%%% plot time-series of emissions reduction magnitudes
%%% no error bars, just shading
%%% REMEMBER TO CLEAR THE IN-LINE OUTPUT AFTER PLOTTING
%%% created: 5/31/2023
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%
%%% inputs
%%%%%%%%%%%%%%%%%%%%%%

% directory with emissions reductions
rel_path_input_emissions_reductions = "../../Data/Emissions Reductions/2. edited";
fn_save_end = ""; % ending of emissions ratio fits
% directory with total cf emissions
rel_path_input_emissions_total_cf = "../../Data/Counterfactual Emissions/8. total cf edited";
% directory with observed emissions
rel_path_observed = "../../Data/Simple Dispatch Outputs/2023-06-23 act/Actual CEMS";
% directory with output figures
rel_path_output = "../../Figures/for publication";

% regions to run for
regions_all = ["ATL", ... % all sites in Atlanta
    "NYC"]; % all sites in NYC
% groups of states to run for; must be parallel to emissions used for "sites"
fn_ends = {["SOCO"], ["NYC"]};
years_all = 2006:2019;

fp = fileparts(matlab.desktop.editor.getActiveFilename);

%%%%%%%%%%%%%%%%%%%%%%
%%% process data
%%%%%%%%%%%%%%%%%%%%%%

%% pre-allocate structures
emissions_so2_total_cf = struct();
emissions_nox_total_cf = struct();
emissions_so2_total_diff = struct();
emissions_nox_total_diff = struct();
emissions_so2_short = struct();
emissions_nox_short = struct();
emissions_so2_CAIR = struct();
emissions_nox_CAIR = struct();
emissions_so2_other = struct();
emissions_nox_other = struct();

% loop through ATL and NYC
for i=1:length(regions_all)
    region = regions_all(i);
    fn_end = fn_ends{i};

    % retrieve emissions reduction magnitudes
    cd(fp)
    cd(rel_path_input_emissions_reductions)
    fn = strcat(strjoin(fn_end, "_"), "_", num2str(years_all(1)),...
        "-", num2str(years_all(end)));
    % short-run
    emissions_so2_short.(region) = parquetread(strcat("so2_", fn, "_short-run.parquet"));
    emissions_nox_short.(region) = parquetread(strcat("nox_", fn, "_short-run.parquet"));
    % CAIR
    emissions_so2_CAIR.(region) = parquetread(strcat("so2_", fn, "_CAIR_reductions", fn_save_end, ".parquet"));
    emissions_nox_CAIR.(region) = parquetread(strcat("nox_", fn, "_CAIR_reductions", fn_save_end, ".parquet"));
    % other
    emissions_so2_other.(region) = parquetread(strcat("so2_", fn, "_other_reductions", fn_save_end, ".parquet"));
    emissions_nox_other.(region) = parquetread(strcat("nox_", fn, "_other_reductions", fn_save_end, ".parquet"));

    % calculate emissions reductions for total cf
    % retrieve total cf
    cd(fp)
    cd(rel_path_input_emissions_total_cf)
    emissions_so2_total_cf.(region) = parquetread(strcat("so2_", strjoin(fn_ends{i}, '_'), '_', num2str(years_all(1)),...
        "-", num2str(years_all(end)), "_bin_daily.parquet"));
    emissions_nox_total_cf.(region) = parquetread(strcat("nox_", strjoin(fn_ends{i}, '_'), '_', num2str(years_all(1)),...
        "-", num2str(years_all(end)), "_bin_daily.parquet"));
    % retrieve observed
    % do first year (if only 1 year, retrieves only first year)
    emissions_obs = retrieve_data(years_all(1), fn_ends{i}, rel_path_observed);
    if length(years_all)~=1 % if range of year,
        for j = 2:length(years_all) % loop through remaining year and append to one large loop
            % read in remaining data
            temp = retrieve_data(years_all(j), fn_ends{i}, rel_path_observed);

            % append to larger dataset
            emissions_obs = [emissions_obs; temp];
        end
    end
    % retime to 1 day resolution
    emissions_obs = table2timetable(emissions_obs);
    mask = isbetween(emissions_obs.Properties.RowTimes, datetime(years_all(1), 1, 1), datetime(years_all(end), 12, 31));
    emissions_obs = retime(emissions_obs(mask, :), 'daily', 'sum');
    emissions_obs = timetable2table(emissions_obs);
    % calculate daily emissions reductions and multiply by -1
    % ensure indices are consistent
    emissions_so2_total_diff.(region) = subtractTableAndMakeNegative( ...
        emissions_so2_total_cf.(region), emissions_obs.so2_tot);
    emissions_nox_total_diff.(region) = subtractTableAndMakeNegative( ...
        emissions_nox_total_cf.(region), emissions_obs.nox_tot);
    
    % save unbinned short-run and total counterfactual diffs
    emissions_so2_short_unbinned.(region) = emissions_so2_short.(region);
    emissions_nox_short_unbinned.(region) = emissions_nox_short.(region);
    emissions_so2_total_unbinned.(region) = emissions_so2_total_diff.(region);
    emissions_nox_total_unbinned.(region) = emissions_nox_total_diff.(region);

    % bin data by month
    % establish time spacing
    spacing = datetime(years_all(1), 1, 1):calmonths(1):datetime(years_all(end)+1, 1, 1);
    spacing = [spacing(1:end-1)', spacing(2:end)'-caldays(1)]; % start and end of bins
    spacing = datenum(spacing); % to datenum for binning function
    % bin all emissions reductions
    emissions_so2_short.(region) = binEmissions(emissions_so2_short.(region), spacing);
    emissions_nox_short.(region) = binEmissions(emissions_nox_short.(region), spacing);
    emissions_so2_CAIR.(region) = binEmissions(emissions_so2_CAIR.(region), spacing);
    emissions_nox_CAIR.(region) = binEmissions(emissions_nox_CAIR.(region), spacing);
    emissions_so2_other.(region) = binEmissions(emissions_so2_other.(region), spacing);
    emissions_nox_other.(region) = binEmissions(emissions_nox_other.(region), spacing);
    emissions_so2_total_diff.(region) = binEmissions(emissions_so2_total_diff.(region), spacing);
    emissions_nox_total_diff.(region) = binEmissions(emissions_nox_total_diff.(region), spacing);
end

%%%%%%%%%%%%%%%%%%%%%%
%%% plot emissions reductions
%%% for all plots, plot all emissions reductions in transparent and then
%%% plot the significant emissions reductions
%%%%%%%%%%%%%%%%%%%%%%

%% figure attributes
plot_short = true;
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

%% 1. SO2
fig_so2 = figure('Position', get(0, 'Screensize')); %for saving the figure
tiledlayout(1, 2, "TileSpacing", "compact");
% a. ATL
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total
p_total = plot(datenum(emissions_so2_total_diff.ATL.Date), emissions_so2_total_diff.ATL.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(emissions_so2_total_diff.ATL.Date), nan_non_significant(emissions_so2_total_diff.ATL)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(4, :))
% short-run
if plot_short
    p_short = plot(datenum(emissions_so2_short.ATL.Date), -1*emissions_so2_short.ATL.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
    p_short.Color(4) = colors_transparency(1);
    plot(datenum(emissions_so2_short.ATL.Date), -1*nan_non_significant(emissions_so2_short.ATL)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(1, :))
end
% CAIR
p_CAIR = plot(datenum(emissions_so2_CAIR.ATL.Date), -1*emissions_so2_CAIR.ATL.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(emissions_so2_CAIR.ATL.Date), -1*nan_non_significant(emissions_so2_CAIR.ATL)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(2, :))
% other
p_other = plot(datenum(emissions_so2_other.ATL.Date), -1*emissions_so2_other.ATL.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(emissions_so2_other.ATL.Date), -1*nan_non_significant(emissions_so2_other.ATL)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(3, :))


% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-4, 0.5])

% labels
ax.FontSize = fontSize;
ylabel({'\DeltaSO_2'; '(Gg/day)'})

hold off

% b. NYC
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total
p_total = plot(datenum(emissions_so2_total_diff.NYC.Date), emissions_so2_total_diff.NYC.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(emissions_so2_total_diff.NYC.Date), nan_non_significant(emissions_so2_total_diff.NYC)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(4, :))
% short-run
if plot_short
    p_short = plot(datenum(emissions_so2_short.NYC.Date), -1*emissions_so2_short.NYC.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
    p_short.Color(4) = colors_transparency(1);
    plot(datenum(emissions_so2_short.NYC.Date), -1*nan_non_significant(emissions_so2_short.NYC)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(1, :))
end
% CAIR
p_CAIR = plot(datenum(emissions_so2_CAIR.NYC.Date), -1*emissions_so2_CAIR.NYC.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(emissions_so2_CAIR.NYC.Date), -1*nan_non_significant(emissions_so2_CAIR.NYC)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(2, :))
% other
p_other = plot(datenum(emissions_so2_other.NYC.Date), -1*emissions_so2_other.NYC.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(emissions_so2_other.NYC.Date), -1*nan_non_significant(emissions_so2_other.NYC)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(3, :))


% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-8, 2])

% labels
ax.FontSize = fontSize;

hold off

%% 2. NOx
fig_nox = figure('Position', get(0, 'Screensize')); %for saving the figure
tiledlayout(1, 2, "TileSpacing", "compact");
% a. ATL
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total
p_total = plot(datenum(emissions_nox_total_diff.ATL.Date), emissions_nox_total_diff.ATL.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(emissions_nox_total_diff.ATL.Date), nan_non_significant(emissions_nox_total_diff.ATL)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(4, :))
% short-run
if plot_short
    p_short = plot(datenum(emissions_nox_short.ATL.Date), -1*emissions_nox_short.ATL.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
    p_short.Color(4) = colors_transparency(1);
    plot(datenum(emissions_nox_short.ATL.Date), -1*nan_non_significant(emissions_nox_short.ATL)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(1, :))
end
% CAIR
p_CAIR = plot(datenum(emissions_nox_CAIR.ATL.Date), -1*emissions_nox_CAIR.ATL.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(emissions_nox_CAIR.ATL.Date), -1*nan_non_significant(emissions_nox_CAIR.ATL)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(2, :))
% other
p_other = plot(datenum(emissions_nox_other.ATL.Date), -1*emissions_nox_other.ATL.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(emissions_nox_other.ATL.Date), -1*nan_non_significant(emissions_nox_other.ATL)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(3, :))


% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-0.7, 0.1])

% labels
ax.FontSize = fontSize;
ylabel({'\DeltaNO_x'; '(Gg/day)'})

hold off

% b. NYC
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% total
p_total = plot(datenum(emissions_nox_total_diff.NYC.Date), emissions_nox_total_diff.NYC.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(4, :));
p_total.Color(4) = colors_transparency(4);
plot(datenum(emissions_nox_total_diff.NYC.Date), nan_non_significant(emissions_nox_total_diff.NYC)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(4, :))
% short-run
if plot_short
    p_short = plot(datenum(emissions_nox_short.NYC.Date), -1*emissions_nox_short.NYC.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(1, :));
    p_short.Color(4) = colors_transparency(1);
    plot(datenum(emissions_nox_short.NYC.Date), -1*nan_non_significant(emissions_nox_short.NYC)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(1, :))
end
% CAIR
p_CAIR = plot(datenum(emissions_nox_CAIR.NYC.Date), -1*emissions_nox_CAIR.NYC.median/1e6, ...
        '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(2, :));
p_CAIR.Color(4) = colors_transparency(2);
plot(datenum(emissions_nox_CAIR.NYC.Date), -1*nan_non_significant(emissions_nox_CAIR.NYC)/1e6, ...
        'LineWidth', lineWidth, 'color', colors(2, :))
% other
p_other = plot(datenum(emissions_nox_other.NYC.Date), -1*emissions_nox_other.NYC.median/1e6, ...
    '-.', 'LineWidth', lineWidth*non_significant_scale, 'color', colors(3, :));
p_other.Color(4) = colors_transparency(3);
plot(datenum(emissions_nox_other.NYC.Date), -1*nan_non_significant(emissions_nox_other.NYC)/1e6, ...
    'LineWidth', lineWidth, 'color', colors(3, :))

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
% ylim([-0.1, 0.4])

% labels
ax.FontSize = fontSize;

hold off

%%%%%%%%%%%%%%%%%%%%%%
%% export
%%%%%%%%%%%%%%%%%%%%%%
fn_beg = "fig_emissions_reductions_magnitudes";
cd(fp)
cd(rel_path_output)
if plot_short
    exportgraphics(fig_so2, strcat(fn_beg, "_so2.png"));
    exportgraphics(fig_nox, strcat(fn_beg, "_nox.png"));
else
    exportgraphics(fig_so2, strcat(fn_beg, "_no_short_so2.png"));
    exportgraphics(fig_nox, strcat(fn_beg, "_no_short_nox.png"));
end

%%%%%%%%%%%%%%%%%%%%%%
%%% additional calculations for publication
%%%%%%%%%%%%%%%%%%%%%%

%% short-run portion of NOx emissions
nox_short_run_ATL = average_year(emissions_nox_short_unbinned.ATL.median./emissions_nox_total_unbinned.ATL.median, ...
    emissions_nox_short_unbinned.ATL.Date, 2019)
nox_short_run_NYC = average_year(emissions_nox_short_unbinned.NYC.median./emissions_nox_total_unbinned.NYC.median, ...
    emissions_nox_short_unbinned.NYC.Date, 2019)

%%
function [output] = binEmissions(input, spacing)
% input is input table that has Date, lower_bound, upper_bound, and median
% spacing is bin edges in datenum

% median
[median, ~, ~, timeSpacing] = BinAvg(datenum(input.Date), input.median, ...
    spacing, 15, 4);
% lower bound
[lower_bound, ~, ~, ~] = BinAvg(datenum(input.Date), input.lower_bound, ...
    spacing, 15, 4);
% upper bound
[upper_bound, ~, ~, ~] = BinAvg(datenum(input.Date), input.upper_bound, ...
    spacing, 15, 4);

output = table(timeSpacing, median, lower_bound, upper_bound);
output.Properties.VariableNames = ["Date", "median", "lower_bound", "upper_bound"];

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