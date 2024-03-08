# %%
import pandas as pd


# %%
metadata1 = pd.read_json('test/snapshot/metadata-1.json')
metadata2 = pd.read_json('test/snapshot/metadata-2.json')
metadata3 = pd.read_json('test/snapshot/metadata-3.json')

content1 = pd.read_json('test/snapshot/content-1.json')
content2 = pd.read_json('test/snapshot/content-2.json')
content3 = pd.read_json('test/snapshot/content-3.json')

labels1 = pd.read_json('test/snapshot/labels-1.json')
labels2 = pd.read_json('test/snapshot/labels-2.json')
labels3 = pd.read_json('test/snapshot/labels-3.json')

# %%
metadata = pd.concat([metadata1, metadata2, metadata3])
metadata.drop(columns=['type_of_material'], inplace=True)
metadata = metadata.drop_duplicates(subset='_id')
metadata.to_json(
    'data/metadata.json',
    orient='records',
    indent=2,
)
# %%
content = pd.concat([content1, content2, content3])
content = content.drop_duplicates(subset=['article_id', 'text'])
content.to_json(
    'data/content.json',
    orient='records',
    indent=2,
)
# %%
labels = pd.concat([labels1, labels2, labels3])
labels = labels.drop_duplicates(subset=['article_id', 'text_id'])
labels.to_json(
    'data/labels.json',
    orient='records',
    indent=2,
)
# %%

df = metadata.merge(
    content,
    left_on='_id',
    right_on='article_id',
    how='outer',
)
df.astype({'text_id': 'Int64'})
# %%

df2 = df.merge(
    labels,
    left_on=['article_id', 'text_id'],
    right_on=['article_id', 'text_id'],
    how='outer',
)
df2 = df2.dropna(subset=['text', 'sentiment'])
df2.info()
df2.to_json(
    'data/ai-mood.json',
    orient='records',
    indent=2,
)

# %%