%%%%%%%%%%%%%%%%%%%%%%
%%% plot time-series of emissions reduction ratios
%%% REMEMBER TO CLEAR THE IN-LINE OUTPUT AFTER PLOTTING
%%% created: 5/31/2023
%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%
%%% inputs
%%%%%%%%%%%%%%%%%%%%%%

% directory with emissions reductions
rel_path_input_emissions_reductions = "../../Data/Emissions Reductions/2. edited";
fn_save_end = ""; % ending of emissions ratio fits
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
emissions_so2_ratios = struct();
emissions_nox_ratios = struct();
emissions_so2_CAIR_fits = struct();
emissions_nox_CAIR_fits = struct();
emissions_so2_other_fits = struct();
emissions_nox_other_fits = struct();
emissions_so2_CAIR_fits_extended = struct();
emissions_nox_CAIR_fits_extended = struct();
emissions_so2_other_fits_extended = struct();
emissions_nox_other_fits_extended = struct();

% loop through ATL and NYC
for i=1:length(regions_all)
region = regions_all(i);
    fn_end = fn_ends{i};

    % retrieve emissions reduction ratios
    cd(fp)
    cd(rel_path_input_emissions_reductions)
    fn = strcat(strjoin(fn_end, "_"), "_", num2str(years_all(1)),...
        "-", num2str(years_all(end)));
    % ratio of emissions difference to total cf
    emissions_so2_ratios.(region) = parquetread(strcat("so2_", fn, "_short-run_normalized.parquet"));
    emissions_nox_ratios.(region) = parquetread(strcat("nox_", fn, "_short-run_normalized.parquet"));

    % non-extended lines
    % CAIR
    emissions_so2_CAIR_fits.(region) = parquetread(strcat("so2_", fn, "_CAIR_reductions_ratios", fn_save_end, ".parquet"));
    % ozone season and non-ozone season for NOx
    emissions_nox_CAIR_fits.(strcat(region, "_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_CAIR_ozone_season_reductions_ratios", fn_save_end, ".parquet"));
    emissions_nox_CAIR_fits.(strcat(region, "_non_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_CAIR_non_ozone_season_reductions_ratios", fn_save_end, ".parquet"));
    % other
    emissions_so2_other_fits.(region) = parquetread(strcat("so2_", fn, "_other_reductions_ratios", fn_save_end, ".parquet"));
    % ozone season and non-ozone season for NOx
    emissions_nox_other_fits.(strcat(region, "_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_other_ozone_season_reductions_ratios", fn_save_end, ".parquet"));
    emissions_nox_other_fits.(strcat(region, "_non_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_other_non_ozone_season_reductions_ratios", fn_save_end, ".parquet"));
    
    % insert nans for off seasons
    emissions_nox_CAIR_fits.(strcat(region, "_ozone_season")) = insertNan(emissions_nox_CAIR_fits.(strcat(region, "_ozone_season")), true);
    emissions_nox_other_fits.(strcat(region, "_ozone_season")) = insertNan(emissions_nox_other_fits.(strcat(region, "_ozone_season")), true);
    emissions_nox_CAIR_fits.(strcat(region, "_non_ozone_season")) = insertNan(emissions_nox_CAIR_fits.(strcat(region, "_non_ozone_season")), false);
    emissions_nox_other_fits.(strcat(region, "_non_ozone_season")) = insertNan(emissions_nox_other_fits.(strcat(region, "_non_ozone_season")), false);
    
    % extended lines
    % CAIR
    emissions_so2_CAIR_fits_extended.(region) = parquetread(strcat("so2_", fn, "_CAIR_reductions_ratios_extended", fn_save_end, ".parquet"));
    % ozone season and non-ozone season for NOx
    ozone_season_extension = parquetread( ...
        strcat("nox_", fn, "_CAIR_ozone_season_reductions_ratios_extended", fn_save_end, ".parquet"));
    % extend ozone season line to end of period
    ozone_season_extension.Date(end + 1) = datetime(years_all(end), 12, 31);
    ozone_season_extension.median(end) = ozone_season_extension.median(end - 1);
    emissions_nox_CAIR_fits_extended.(strcat(region, "_ozone_season")) = ozone_season_extension;
    emissions_nox_CAIR_fits_extended.(strcat(region, "_non_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_CAIR_non_ozone_season_reductions_ratios_extended", fn_save_end, ".parquet"));
    % other
    emissions_so2_other_fits_extended.(region) = parquetread(strcat("so2_", fn, "_other_reductions_ratios_extended", fn_save_end, ".parquet"));
    % ozone season and non-ozone season for NOx
    ozone_season_extension = parquetread( ...
        strcat("nox_", fn, "_other_ozone_season_reductions_ratios_extended", fn_save_end, ".parquet"));
    % extend ozone season line to end of period
    ozone_season_extension.Date(end + 1) = datetime(years_all(end), 12, 31);
    ozone_season_extension.median(end) = ozone_season_extension.median(end - 1);
    emissions_nox_other_fits_extended.(strcat(region, "_ozone_season")) = ozone_season_extension;
    emissions_nox_other_fits_extended.(strcat(region, "_non_ozone_season")) = parquetread( ...
        strcat("nox_", fn, "_other_non_ozone_season_reductions_ratios_extended", fn_save_end, ".parquet"));
end

%%%%%%%%%%%%%%%%%%%%%%
%%% plot emissions reductions
%%%%%%%%%%%%%%%%%%%%%%

%% figure attributes
colors_ratios = [0, 158, 115]/255; 
% make the fit colors a little brighter than the ones for the emission
% reductions themselves
colors_fits = [230, 159, 0; % yellow, CAIR
                150, 150, 150;]/255; % gray, other
xlims = datetime([2006, 2020], 1, 1);
xtickss = datetime(2006, 1, 1):calyears(2):datetime(2020, 1, 1);
fontSize = 30;
lineWidth = 6;
aspect_ratio = [3, 1, 1];

%% 1. SO2
fig_so2 = figure('Position', get(0, 'Screensize')); %for saving the figure
tiledlayout(1, 2, "TileSpacing", "compact");
% a. ATL
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% emissions ratios
[hl, ~] = boundedline(datenum(emissions_so2_ratios.ATL.Date), emissions_so2_ratios.ATL.median, ...
    [emissions_so2_ratios.ATL.median - emissions_so2_ratios.ATL.lower_bound, ...
    emissions_so2_ratios.ATL.upper_bound - emissions_so2_ratios.ATL.median], 'LineWidth', 1,...
    'nan', 'gap', 'alpha', 'transparency', 0.2, ...
    'color', colors_ratios);
hl.Color(4) = 0.35; % make transparent

% CAIR
% plot extended line
p1 = plot(datenum(emissions_so2_CAIR_fits_extended.ATL.Date), emissions_so2_CAIR_fits_extended.ATL.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_so2_CAIR_fits.ATL.Date), emissions_so2_CAIR_fits.ATL.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));

% other
plot(datenum(emissions_so2_other_fits_extended.ATL.Date), emissions_so2_other_fits_extended.ATL.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-1, 0.5])
ax.YTickLabel = {'-100', '-50', '0', '50'};

% labels
ax.FontSize = fontSize;

hold off

% b. NYC
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% emissions ratios
[hl, ~] = boundedline(datenum(emissions_so2_ratios.NYC.Date), emissions_so2_ratios.NYC.median, ...
    [emissions_so2_ratios.NYC.median - emissions_so2_ratios.NYC.lower_bound, ...
    emissions_so2_ratios.NYC.upper_bound - emissions_so2_ratios.NYC.median], 'LineWidth', 1,...
    'nan', 'gap', 'alpha', 'transparency', 0.2, ...
    'color', colors_ratios);
hl.Color(4) = 0.35; % make transparent

% CAIR
% plot extended line
p1 = plot(datenum(emissions_so2_CAIR_fits_extended.NYC.Date), emissions_so2_CAIR_fits_extended.NYC.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_so2_CAIR_fits.NYC.Date), emissions_so2_CAIR_fits.NYC.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));

% other
plot(datenum(emissions_so2_other_fits_extended.NYC.Date), emissions_so2_other_fits_extended.NYC.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-1, 0.5])
ax.YTickLabel = {'-100', '-50', '0', '50'};

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

% emissions ratios
[hl, ~] = boundedline(datenum(emissions_nox_ratios.ATL.Date), emissions_nox_ratios.ATL.median, ...
    [emissions_nox_ratios.ATL.median - emissions_nox_ratios.ATL.lower_bound, ...
    emissions_nox_ratios.ATL.upper_bound - emissions_nox_ratios.ATL.median], 'LineWidth', 1,...
    'nan', 'gap', 'alpha', 'transparency', 0.2, ...
    'color', colors_ratios);
hl.Color(4) = 0.35; % make transparent

% CAIR
% plot extended line
% ozone season
p1 = plot(datenum(emissions_nox_CAIR_fits_extended.ATL_ozone_season.Date), emissions_nox_CAIR_fits_extended.ATL_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_nox_CAIR_fits.ATL_ozone_season.Date), emissions_nox_CAIR_fits.ATL_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));
% ozone season
p2 = plot(datenum(emissions_nox_CAIR_fits_extended.ATL_non_ozone_season.Date), emissions_nox_CAIR_fits_extended.ATL_non_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p2.Color(4) = 0.3;
plot(datenum(emissions_nox_CAIR_fits.ATL_non_ozone_season.Date), emissions_nox_CAIR_fits.ATL_non_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));


% other
% ozone season
p1 = plot(datenum(emissions_nox_other_fits_extended.ATL_ozone_season.Date), emissions_nox_other_fits_extended.ATL_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(2, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_nox_other_fits.ATL_ozone_season.Date), emissions_nox_other_fits.ATL_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));
% ozone season
p2 = plot(datenum(emissions_nox_other_fits_extended.ATL_non_ozone_season.Date), emissions_nox_other_fits_extended.ATL_non_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(2, :));
p2.Color(4) = 0.3;
plot(datenum(emissions_nox_other_fits.ATL_non_ozone_season.Date), emissions_nox_other_fits.ATL_non_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-1, 0.5])
ax.YTickLabel = {'-100', '-50', '0', '50'};

% labels
ax.FontSize = fontSize;
xlabel("Year")
% ylabel({'\Deltanox'; '(Gg/day)'})

hold off

% b. NYC
ax = nexttile;
hold on
box on
pbaspect(aspect_ratio)

yline(0, '-', 'Color', [0.7, 0.7, 0.7], LineWidth=0.5)

% emissions ratios
[hl, ~] = boundedline(datenum(emissions_nox_ratios.NYC.Date), emissions_nox_ratios.NYC.median, ...
    [emissions_nox_ratios.NYC.median - emissions_nox_ratios.NYC.lower_bound, ...
    emissions_nox_ratios.NYC.upper_bound - emissions_nox_ratios.NYC.median], 'LineWidth', 1,...
    'nan', 'gap', 'alpha', 'transparency', 0.2, ...
    'color', colors_ratios);
hl.Color(4) = 0.35; % make transparent

% CAIR
% plot extended line
% ozone season
p1 = plot(datenum(emissions_nox_CAIR_fits_extended.NYC_ozone_season.Date), emissions_nox_CAIR_fits_extended.NYC_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_nox_CAIR_fits.NYC_ozone_season.Date), emissions_nox_CAIR_fits.NYC_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));
% ozone season
p2 = plot(datenum(emissions_nox_CAIR_fits_extended.NYC_non_ozone_season.Date), emissions_nox_CAIR_fits_extended.NYC_non_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(1, :));
p2.Color(4) = 0.3;
plot(datenum(emissions_nox_CAIR_fits.NYC_non_ozone_season.Date), emissions_nox_CAIR_fits.NYC_non_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(1, :));


% other
% ozone season
p1 = plot(datenum(emissions_nox_other_fits_extended.NYC_ozone_season.Date), emissions_nox_other_fits_extended.NYC_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(2, :));
p1.Color(4) = 0.3;
plot(datenum(emissions_nox_other_fits.NYC_ozone_season.Date), emissions_nox_other_fits.NYC_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));
% ozone season
p2 = plot(datenum(emissions_nox_other_fits_extended.NYC_non_ozone_season.Date), emissions_nox_other_fits_extended.NYC_non_ozone_season.median, ...
    '-', 'LineWidth', lineWidth, 'color', colors_fits(2, :));
p2.Color(4) = 0.3;
plot(datenum(emissions_nox_other_fits.NYC_non_ozone_season.Date), emissions_nox_other_fits.NYC_non_ozone_season.median, ...
    'LineWidth', lineWidth, 'color', colors_fits(2, :));

% lims and ticks
xlim(datenum(xlims))
xticks(datenum(xtickss))
ax.XTickLabel = datestr(xtickss, "yyyy");
ylim([-1, 0.5])
ax.YTickLabel = {'-100', '-50', '0', '50'};

% labels
ax.FontSize = fontSize;
% ylabel({'\Deltanox'; '(Gg/day)'})

hold off
%% 
%%%%%%%%%%%%%%%%%%%%%%
%%% export
%%%%%%%%%%%%%%%%%%%%%%
fn_beg = "fig_emissions_reductions_ratios";
cd(fp)
cd(rel_path_output)

exportgraphics(fig_so2, strcat(fn_beg, "_so2.png"));
exportgraphics(fig_nox, strcat(fn_beg, "_nox.png"));

%% additional calculations for publication

% fractional reductions for CAIR
so2_CAIR_ATL = emissions_so2_CAIR_fits_extended.ATL.median(end);
so2_CAIR_NYC = emissions_so2_CAIR_fits_extended.NYC.median(end);
nox_CAIR_ATL_ozone_season = emissions_nox_CAIR_fits_extended.ATL_ozone_season.median(end);
nox_CAIR_NYC_ozone_season = emissions_nox_CAIR_fits_extended.NYC_ozone_season.median(end);
nox_CAIR_ATL_non_ozone_season = emissions_nox_CAIR_fits_extended.ATL_non_ozone_season.median(end);
nox_CAIR_NYC_non_ozone_season = emissions_nox_CAIR_fits_extended.NYC_non_ozone_season.median(end);

% fractional reductions for other
so2_other_ATL = emissions_so2_other_fits_extended.ATL.median(end);
so2_other_NYC = emissions_so2_other_fits_extended.NYC.median(end);
nox_other_ATL_ozone_season = emissions_nox_other_fits_extended.ATL_ozone_season.median(end);
nox_other_NYC_ozone_season = emissions_nox_other_fits_extended.NYC_ozone_season.median(end);
nox_other_ATL_non_ozone_season = emissions_nox_other_fits_extended.ATL_non_ozone_season.median(end);
nox_other_NYC_non_ozone_season = emissions_nox_other_fits_extended.NYC_non_ozone_season.median(end);
% other need to be subtracted by CAIR
so2_other_ATL = so2_other_ATL - so2_CAIR_ATL;
so2_other_NYC = so2_other_NYC - so2_CAIR_NYC;
nox_other_ATL_ozone_season = nox_other_ATL_ozone_season - nox_CAIR_ATL_ozone_season;
nox_other_NYC_ozone_season = nox_other_NYC_ozone_season - nox_CAIR_NYC_ozone_season;
nox_other_ATL_non_ozone_season = nox_other_ATL_non_ozone_season - nox_CAIR_ATL_non_ozone_season;
nox_other_NYC_non_ozone_season = nox_other_NYC_non_ozone_season - nox_CAIR_NYC_non_ozone_season;
%%
function [output] = insertNan(input, ozoneSeason)
if ozoneSeason
    gap_month = 10;
    gap_day = 15;
else
    gap_month = 6;
    gap_day = 15;
end
start_year = year(input.Date(1));
output = input;
for year_to_insert = start_year:2019
    newRow = array2table(nan(1, width(input)));
    newRow.Properties.VariableNames = input.Properties.VariableNames;
    newRow.Date = datetime(year_to_insert, gap_month, gap_day);
    output = [output; newRow];
end
output = sortrows(output, 'Date');
end