import time
import pandas as pd
from tqdm import tqdm
tqdm.pandas()
from sentence_transformers import SentenceTransformer
sbert_model = SentenceTransformer('bert-base-nli-mean-tokens')
import multiprocessing as mp
import numpy as np
# mp.set_start_method('forkserver', force=True)

EMBEDDED_DATA = '/home/ubuntu/nishant/dmx-rnd/dexter_labs/01_tag_recommed/scripts_nb/embed_data/embedded_BERT_article.csv'
LABEL_DATA = 'clean_v2_train_data.csv'
start_chunk_size = 100000
end_chunk_size = start_chunk_size +500000
n_core = 11

def get_embedding(df_data):
    df_data['embed'] = df_data['clean_article'].progress_apply(lambda x : sbert_model.encode([x])[0])
    return df_data

def parallelize_dataframe(df, func, n_cores=n_core):
    df_split = np.array_split(df, n_cores)
    pool = mp.Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

try:
     mp.set_start_method('spawn')
except RuntimeError:
    pass
    # mp.set_start_method('forkserver', force=True)

if __name__ == '__main__':
    try:
        col_to_embed = 'clean_article'
        # df_data = pd.read_csv(LABEL_DATA)[start_chunk_size:]
        df_data = pd.read_csv(LABEL_DATA)
        df_data.dropna(inplace=True)
        df_data.reset_index(drop=True,inplace=True)

        # Data prepration
        print('Sentence Embedding in progress...')
        start_time = time.time()
        print('parallel run started')
        df_data = parallelize_dataframe(df_data, get_embedding)
        print('parallel run comepletes')

        print('loading embedding in dataframe')
        sentence_embeddings = df_data['embed'].to_list()
        df_embed= pd.DataFrame(sentence_embeddings)
        df_data.drop(columns=['embed'],inplace=True)

        print('Creating Modeling data')
        df_modeling_data = df_data.merge(df_embed,left_index=True,right_index=True)
        df_data = pd.DataFrame()
        df_modeling_data.to_csv(EMBEDDED_DATA,index=False)

        finish_time = round((time.time()-start_time)/60,3)
        print(f'Embedding completed in {finish_time} mins')

    except Exception as ex :
        print(ex)
