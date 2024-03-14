import json
import os
import pathlib
import re
from cleantext import clean
from datetime import datetime
import hashlib
from util import SPANISH_FLAGGED_WORDS, TEXT_LENGTH_PATTERN, ANONYMIZE_URL_MAIL

corpus_folder = 'data'
output_folder = 'output_compact'
output_verbose_folder = 'output_verbose'

def clean_text(text):
    text = clean(text,
          fix_unicode=True,  # fix various unicode errors
          to_ascii=False,  # transliterate to closest ASCII representation
          lower=False,  # lowercase text
          no_line_breaks=False,  # fully strip line breaks as opposed to only normalizing them
          no_urls=True,  # replace all URLs with a special token
          no_emails=True,  # replace all email addresses with a special token
          no_phone_numbers=True,  # replace all phone numbers with a special token
          no_numbers=False,  # replace all numbers with a special token
          no_digits=False,  # replace all digits with a special token
          no_currency_symbols=False,  # replace all currency symbols with a special token
          no_punct=False,  # remove punctuations
          replace_with_punct="",  # instead of removing punctuations you may replace them
          replace_with_url="<URL>",
          replace_with_email="<EMAIL>",
          replace_with_phone_number="<PHONE>",
          replace_with_number="<NUMBER>",
          replace_with_digit="0",
          replace_with_currency_symbol="<CUR>",
          lang="es"  # set to 'de' for German special handling
          )
    text = re.sub(r"<URL>(\n<URL>)?", "<URL>", text)
    return text

def check_text_length(comment):
    processed_text = re.sub(ANONYMIZE_URL_MAIL, "", comment['body'])

    # Check if the length of the processed text is more than 10
    if len(processed_text) > 10:
        return True
    return False

def check_flagged_words(comment):
    for word in SPANISH_FLAGGED_WORDS:
        if word in comment["body"].lower():
            return True
    return False

def get_children(comments_list):
    children = {}
    for comment in comments_list:
        if comment['parent_id'].split('_')[0] == 't1':
            parent_id = comment['parent_id'].split('_')[1]
            if parent_id not in children:
                children[parent_id] = []
            children[parent_id].append(comment['id'])
    children[-1] = [comment['id'] for comment in comments_list if comment['parent_id'].split('_')[0] == 't3']
    return children

def find_paths(node, current_path, tree, all_paths):
    current_path.append(node)

    # If the node has no children (it's a leaf node)
    if node not in tree or not tree[node]:
        all_paths.append(current_path.copy())
        return

    # If the node has children, iterate over each child and recursively find paths
    for child in tree[node]:
        find_paths(child, current_path, tree, all_paths)
        current_path.pop()  # backtrack

def get_all_paths(tree, root):
    all_paths = []
    find_paths(root, [], tree, all_paths)
    return all_paths

def main():
    files = os.listdir(corpus_folder)
    pathlib.Path(output_folder).mkdir(parents=True, exist_ok=True)
    pathlib.Path(output_verbose_folder).mkdir(parents=True, exist_ok=True)

    processed_files = os.listdir(output_folder)
    pathlib.Path(output_folder).mkdir(parents=True, exist_ok=True)
    for file in files:
        if file in processed_files:
            continue

    source_files = os.listdir(corpus_folder)
    files_to_process = [file for file in source_files if file.endswith('_comments.jsonl')]
    for file in files_to_process:
        print('Processing subreddit', file, '...')
        path_to_compact = os.path.join(output_folder, file.split("_")[0])
        path_to_verbose = os.path.join(output_verbose_folder, file.split("_")[0])
        print(path_to_compact)
        pathlib.Path(path_to_compact).mkdir(parents=True, exist_ok=True)
        pathlib.Path(path_to_verbose).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(corpus_folder, file), 'r') as f:
            data = f.readlines()
        comments = []
        for line in data:
            comments.append(json.loads(line))
        comments = [comment for comment in comments if comment['body'] not in ['[deleted]', '[removed]']]
        comments_dict = {}
        for comment in comments:
            if comment['link_id'] not in comments_dict:
                comments_dict[comment['link_id']] = []
            comments_dict[comment['link_id']].append(comment)
        print(f'Links found {len(comments_dict)}')
        for link_id in comments_dict:
            print(f'Processing link {link_id}')
            comments_to_filter = []
            for comment in comments_dict[link_id]:
                if not check_text_length(comment) or check_flagged_words(comment):
                    comments_to_filter.append(comment['id'])

            children = get_children(comments_dict[link_id])
            root = -1
            all_paths = get_all_paths(children, root)
            print(f'All paths found {len(all_paths)}')

            if len(all_paths) > 100000:
                print(f'File {file} has too many paths. Skipping...')
                continue

            result = []

            for i in all_paths:
                if all(i not in j or i == j for j in all_paths):
                    result.append(i)

            # Remove duplicates
            dialogues = [list(t) for t in set(tuple(i) for i in result)]
            dialogues = [d[1:] for d in dialogues if
                         len(set(comments_to_filter).intersection(set(d))) == 0 and len(d) > 2]
            print(f'Filtered dialogues {len(dialogues)}')
            if dialogues:
                unique_comments = set([item for sublist in dialogues for item in sublist])
                unique_comments_list = [c for c in comments_dict[link_id] if c['id'] in unique_comments]
                unique_comments_list = [{'id': comment['id'], 'user': hashlib.sha256(comment["author"].encode()).hexdigest()[:8],
                                         'text': clean_text(comment['body']),
                                         'date': datetime.utcfromtimestamp(int(comment['created_utc'])).strftime('%Y-%m-%d %H:%M:%S')}
                                        for comment in
                                        unique_comments_list]
                dialogues_to_json = {i: d for i, d in enumerate(dialogues)}
                dialogues_compact_to_json = {'comments': unique_comments_list, 'dialogues': dialogues_to_json}
                dialogues_verbose_to_json = {}
                for i in dialogues_to_json:
                    dialogue_verbose = []
                    for order in dialogues_to_json[i]:
                        comment = [c for c in comments_dict[link_id] if c['id'] == order][0]
                        dialogue_verbose.append(clean_text(comment['body']))
                    dialogues_verbose_to_json[i] = dialogue_verbose

                with open(os.path.join(path_to_compact, f'{link_id.strip("t3_")}.json'), 'w') as f:
                    json.dump(dialogues_compact_to_json, f)
                with open(os.path.join(path_to_verbose, f'{link_id.strip("t3_")}.json'), 'w') as f:
                    json.dump(dialogues_verbose_to_json, f)

if __name__ == "__main__":
    main()
