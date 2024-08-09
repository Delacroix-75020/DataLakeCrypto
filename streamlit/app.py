import streamlit as st
import yfinance as yf
import pandas as pd
from sklearn.linear_model import LinearRegression
import time

# Titre de l'application
st.title("Prédiction du prix du Bitcoin en Temps Réel")

# Fonction pour charger les données
def load_data():
    data = yf.download('BTC-USD', period='1d', interval='1m')
    data['Return'] = data['Close'].pct_change()
    data = data.dropna()
    return data

# Chargement initial des données
data = load_data()

# Affichage des données récentes
st.subheader('Données récentes du Bitcoin')
st.line_chart(data['Close'])

# Entraînement du modèle initial
model = LinearRegression()
X = data[['Open', 'High', 'Low', 'Volume', 'Return']]
y = data['Close']
model.fit(X, y)

# Prédiction en temps réel
st.subheader("Prédiction en temps réel")

# Boucle pour la mise à jour en temps réel
while True:
    # Récupération des nouvelles données
    data = load_data()
    
    # Affichage des dernières données
    st.write("Dernier prix : $", data['Close'].iloc[-1])

    # Prédiction du prix à partir des dernières données
    latest_data = data.iloc[-1][['Open', 'High', 'Low', 'Volume', 'Return']].values.reshape(1, -5)
    prediction = model.predict(latest_data)
    
    # Affichage de la prédiction
    st.write(f"Le prix prédit du Bitcoin est : ${prediction[0]:.2f}")
    
    # Mise à jour toutes les 60 secondes
    time.sleep(5)
    st.experimental_rerun()
