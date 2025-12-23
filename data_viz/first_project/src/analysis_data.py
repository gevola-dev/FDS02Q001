import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Carica dataset
df = pd.read_csv('crim_pris_cap-defaultview_linear.csv', sep=',')

# Pulizia colonne
df['TIME'] = pd.to_numeric(df['TIME_PERIOD'], errors='coerce')
df['VALUE'] = pd.to_numeric(df['OBS_VALUE'], errors='coerce')

# Filtra solo dati rilevanti per prisons
prison_df = df[df['indic_cr'].str.contains('prison', case=False, na=False)]

print("📊 DIMensioni dataset:", prison_df.shape)
print("\n🏷️ Países unici:", prison_df['geo'].nunique())
print("\n📅 Anni coperti:", sorted(prison_df['TIME'].dropna().unique()))
print("\n🔍 Variabili principali:")
print(prison_df['indic_cr'].value_counts().head())

# Calcola occupancy rate = (prisoners/capacity)*100 per ogni paese/anno
prisoners = prison_df[prison_df['indic_cr'] == 'Actual number of persons held in prison']
capacity = prison_df[prison_df['indic_cr'] == 'Official prison capacity - persons']

print("\n✅ Dati prisoners trovati:", len(prisoners))
print("✅ Dati capacity trovati:", len(capacity))

# Salva CSV pulito per visualizzazioni
prison_df.to_csv('prisons_clean.csv', index=False)
print("\n💾 File 'prisons_clean.csv' salvato!")
