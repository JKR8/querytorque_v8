import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import io
import re

# Raw data processing
data_str = """
query092_multi,8043.91,rewrite,8043.91,-,WIN
query032_multi,1465.16,rewrite,1465.16,-,WIN
query081_multi,438.93,rewrite,438.93,1.42,WIN
query010_multi,30.18,rewrite,30.18,1.42,WIN
query039_multi,29.48,rewrite,29.48,-,WIN
query001_multi,27.80,rewrite,27.80,-,WIN
query101_spj_spj,11.47,rewrite,11.47,-,WIN
query101_agg,10.92,rewrite,10.92,-,WIN
query100_spj_spj,9.09,config,0.61,1.89,RECOVERED
query083_multi,8.56,rewrite,8.56,1.68,WIN
query072_spj_spj,7.18,rewrite,7.18,1.32,WIN
query013_agg,7.02,rewrite,7.02,-,WIN
query102_spj_spj,5.95,config,0.51,1.83,RECOVERED
query100_agg,5.71,config,1.27,1.82,WIN
query072_agg,5.35,config,-,1.81,WIN
query064_multi,3.81,rewrite,3.81,1.17,WIN
query027_agg,3.76,config,0.46,1.73,RECOVERED
query025_agg,3.10,rewrite,3.10,-,WIN
query014_multi,3.02,config,1.98,1.67,WIN
query058_multi,2.51,config,-,1.60,WIN
query027_spj_spj,2.35,config,0.43,1.58,RECOVERED
query102_agg,2.11,config,1.26,1.52,WIN
query059_multi,2.07,rewrite,2.07,1.05,WIN
query069_multi,1.98,rewrite,1.98,1.18,WIN
query030_multi,1.86,rewrite,1.86,1.12,WIN
query075_multi,1.85,config,0.30,1.46,RECOVERED
query031_multi,1.79,rewrite,1.79,-,WIN
query038_multi,1.78,rewrite,1.78,-,WIN
query065_multi,1.75,rewrite,1.75,1.09,WIN
query050_spj_spj,1.39,config,-,1.28,IMPROVED
query080_multi,1.39,rewrite,1.39,-,IMPROVED
query091_spj_spj,1.21,config,-,1.17,IMPROVED
query091_agg,1.19,config,1.18,1.16,IMPROVED
query099_agg,1.18,rewrite,1.18,-,IMPROVED
query087_multi,1.15,rewrite,1.15,1.11,IMPROVED
query050_agg,1.09,rewrite,1.09,-,IMPROVED
query084_spj_spj,1.08,config,1.04,1.07,IMPROVED
query018_agg,1.07,rewrite,1.07,-,IMPROVED
query023_multi,1.07,config,1.01,1.06,IMPROVED
query025_spj_spj,1.06,rewrite,1.06,-,IMPROVED
query018_spj_spj,1.04,rewrite,1.04,-,neutral
query085_agg,1.04,rewrite,1.04,-,neutral
query013_spj_spj,1.03,rewrite,1.03,-,neutral
query019_agg,1.02,rewrite,1.02,-,neutral
query019_spj_spj,1.02,rewrite,1.02,-,neutral
query094_multi,1.02,rewrite,1.02,-,neutral
query040_spj_spj,1.01,rewrite,1.01,-,neutral
query054_multi,1.01,rewrite,1.01,-,neutral
query040_agg,1.00,none,0.97,-,neutral
query084_agg,1.00,none,0.92,-,neutral
query085_spj_spj,1.00,none,-,-,neutral
query099_spj_spj,1.00,none,0.97,-,neutral
"""

# Convert to DataFrame
rows = [line.split(',') for line in data_str.strip().split('\n')]
df = pd.DataFrame(rows, columns=['Query', 'Best_Speedup', 'Best_Source', 'Rewrite_Speedup', 'Config_Speedup', 'Verdict'])

# Clean numeric columns
# Helper to clean strings
def clean_num(x):
    if '-' in x: return np.nan
    try:
        return float(x)
    except:
        return np.nan

df['Best_Speedup'] = df['Best_Speedup'].apply(clean_num)
df['Rewrite_Speedup'] = df['Rewrite_Speedup'].apply(clean_num)
df['Config_Speedup'] = df['Config_Speedup'].apply(clean_num)

# Impute 1.0 (baseline) for missing rewrite/config values for plotting purposes
# If Rewrite is NaN, it implies the rewrite wasn't applied or failed to compile? Or just wasn't the source?
# Based on the prompt data: "query085_spj_spj" has "none" for rewrite. Let's assume 1.0 (no change) if missing for the plot, or drop.
# Actually, for the "Rescue" plots, we need the 0.x values.
df['Plot_Rewrite'] = df['Rewrite_Speedup'].fillna(1.0)
df['Plot_Config'] = df['Config_Speedup'].fillna(1.0)

# Categorize "Strategy"
def get_strategy(row):
    if row['Verdict'] == 'RECOVERED':
        return 'Rescue (Config fixes Rewrite)'
    elif row['Best_Source'] == 'rewrite' and not pd.isna(row['Config_Speedup']):
         return 'Boost (Rewrite + Config)'
    elif row['Best_Source'] == 'rewrite':
        return 'Rewrite Only'
    elif row['Best_Source'] == 'config':
        return 'Config Only'
    else:
        return 'Neutral/None'

df['Strategy'] = df.apply(get_strategy, axis=1)

# --- Visualization 1: Scatter Plot (Rewrite vs. Final Impact) ---
plt.figure(figsize=(12, 8))
sns.set_style("whitegrid")

# Create a scatter plot
# We use log scale because of the outliers
ax = sns.scatterplot(
    data=df,
    x='Plot_Rewrite',
    y='Best_Speedup',
    hue='Strategy',
    style='Strategy',
    s=150, # Marker size
    palette='viridis',
    alpha=0.8
)

# Add diagonal line (where Final = Rewrite)
line_limit = max(df['Best_Speedup'].max(), df['Plot_Rewrite'].max())
plt.plot([0.1, line_limit * 1.5], [0.1, line_limit * 1.5], 'r--', alpha=0.3, label='No Improvement over Rewrite')

# Log scale
plt.xscale('log')
plt.yscale('log')

# Labels and Title
plt.title('Impact of Config Tuning on SQL Rewrites (Log Scale)', fontsize=16, fontweight='bold')
plt.xlabel('Initial Rewrite Speedup (x)', fontsize=12)
plt.ylabel('Final Best Speedup (x)', fontsize=12)

# Annotate interesting points
# Rescue points (Rewrite < 1, Final > 1)
rescue_points = df[df['Strategy'] == 'Rescue (Config fixes Rewrite)']
for _, row in rescue_points.iterrows():
    plt.text(row['Plot_Rewrite'], row['Best_Speedup'], f"{row['Query']}", 
             horizontalalignment='right', size='small', color='black', weight='semibold')

# Top Outliers
top_points = df.nlargest(2, 'Best_Speedup')
for _, row in top_points.iterrows():
    plt.text(row['Plot_Rewrite'], row['Best_Speedup'], f"{row['Query']}", 
             horizontalalignment='left', verticalalignment='bottom', size='medium', color='black')

plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True, which="both", ls="--", alpha=0.2)
plt.tight_layout()
plt.savefig('scatter_impact.png')

# --- Visualization 2: Dumbbell Plot for "Rescue" & "Boost" ---
# Filter for queries where Config made a difference (Rescue or Boost or Config Only > Rewrite)
# Logic: If Best > Rewrite (significantly) OR Strategy is Rescue
# We want to show the jump from Rewrite -> Best
interactive_df = df[df['Strategy'].isin(['Rescue (Config fixes Rewrite)', 'Boost (Rewrite + Config)', 'Config Only'])].copy()
interactive_df = interactive_df.sort_values('Best_Speedup', ascending=True)

# Keep top 15 of these for readability
interactive_df_top = interactive_df.tail(20)

plt.figure(figsize=(12, 10))

# Create the Dumbbell plot lines
plt.hlines(y=interactive_df_top['Query'], xmin=interactive_df_top['Plot_Rewrite'], xmax=interactive_df_top['Best_Speedup'], color='grey', alpha=0.5)

# Plot the points
# Start Point (Rewrite)
plt.scatter(interactive_df_top['Plot_Rewrite'], interactive_df_top['Query'], color='red', alpha=0.7, label='Initial Rewrite', s=100)
# End Point (Best)
plt.scatter(interactive_df_top['Best_Speedup'], interactive_df_top['Query'], color='green', alpha=0.9, label='Final (w/ Config)', s=100)

plt.xscale('log')
plt.title('The "Safety Net": How Config Tuning Rescues or Boosts Rewrites', fontsize=16, fontweight='bold')
plt.xlabel('Speedup (Log Scale)', fontsize=12)
plt.ylabel('Query', fontsize=12)
plt.legend()
plt.grid(True, axis='x', which='both', ls='--', alpha=0.2)
plt.tight_layout()
plt.savefig('dumbbell_rescue.png')

print("Charts generated.")