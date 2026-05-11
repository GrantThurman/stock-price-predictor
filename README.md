# Stock Price Predictor

Predicting the stock price of Lockheed Martin (LMT) using a hybrid CNN-LSTM model.

## Data Preprocessing 📁
All raw data is in the `data` folder. This data is used in `processdata.py`, which joins the data into a CSV called `final_data.csv` and six CSVs for training, validation, and testing of the neural networks. These CSVs can all be found in the `clean_data` folder

## Blog Post Figures 📊
All figures used in the blog post can be found in the `blog_figures` folder. The code to generate figures related to loss or accuracy of models can be found in the `predictor.ipynb` notebook. Any other figure was generated with code found in the `plots.ipynb` notebook. 
