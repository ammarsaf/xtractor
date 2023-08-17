import time
import random
import json
import requests
import logging
import concurrent.futures
from xtractor.headers import AllHeader
from bs4 import BeautifulSoup

head = AllHeader()


def request_page(link: str, delay_time: int = 2, use_random_header=False) -> object:
    """
    Given link, return html text extracted from website

    :param link:str
    :param use_random_header:Boolear = False

    >>> link_harakah = "https://harakahdaily.net/index.php/2018/04/29/senarai-calon-bertanding-pru14/"
    >>> assert type(request_text(link_harakah)) == str, "Not return string"

    """
    if use_random_header:
        random_header = random.choice(head.headers_list)
    else:
        random_header = head.headers_list[0]

    robust_header = head.robust_header
    header_list = [{"User-Agent": random_header}, robust_header]

    time.sleep(delay_time)
    response = requests.get(link, headers=random.choice(header_list))
    return response


def get_text(response: object) -> str:
    """
    Given response from request_text, return html text

    >>> assert type(get_text(response)) == str, "Function not return string"
    """
    html_text = response.text

    assert type(html_text) == str, "request_text not return a string"
    # print("TEXT:\n", html_text)
    return html_text


def bs4_soup(html_text: str) -> object:
    """
    Given html_text, return soup object
    """
    soup_ = BeautifulSoup(html_text, "lxml")
    return soup_


def extractor(link: str) -> object:
    """
    Given list of link, it will return a soup object
    for html tag processing

    :param link:str
    """
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
    response = request_page(link)
    logging.info(f"Link: {link} -- status {response.status_code}")

    html_text = get_text(response)
    soup_ = bs4_soup(html_text)
    return soup_


def multi_threading(
    todo: object, links: list = False, links_dict: dict = False, worker_num=3
) -> list:
    """
    Function that wraped the extractor function to apply
    multithreading processing for faster extraction.

    :param todo: object | function
    :param links: list
    :param links_dict: dict
    :return final_output: list

    """
    final_output = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_num) as executor:
        if not links_dict:
            text_html = {
                executor.submit(todo, idx, url): url for idx, url in enumerate(links)
            }
        else:
            text_html = {
                executor.submit(todo, link): link
                for link in links_dict["article_links"]
            }
        for future in concurrent.futures.as_completed(text_html):
            # url = text_html[future]
            try:
                final_output.append(future.result())
            except Exception as e:
                print(f"Error: {e}")
    return final_output


def dumps_the_json(content: dict, json_file_name: str, ascii_bool: bool = False):
    """
    Given the content:dict and json_file_name,
    return json file in the project path

    :param content:dict
    :param json_file_name:str
    :return json file

    """
    with open(json_file_name, "w") as file:
        json.dump(content, file, ensure_ascii=ascii_bool)


def read_the_json(json_file: str):
    """
    Given json_file, return the content
    of the json_file

    :param json_file:str
    :return link_to_extract:list
    """
    with open(json_file, "r") as file:
        link_to_extract = json.load(file)
    return link_to_extract


def jsonl_converter(
    json_file_path: str,
    json_l_file_path: str,
    col_1_name: str,
    col_2_name: str,
    ascii_bool=False,
):
    with open(json_file_path, "r") as input_file:
        data = json.load(input_file)

    with open(json_l_file_path, "w") as output_file:
        for index, col in zip(data[col_1_name], data[col_2_name]):
            if col is not None:
                output_line = json.dumps(
                    {"index": index, "col": col}, ensure_ascii=ascii_bool
                )
                output_file.write(output_line + "\n")
