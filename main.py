import os
from flask import Flask, make_response, render_template, request

from querry import *
from utils import *

# application Flask
script_dir = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=f"{script_dir}/templates", static_folder=f"{script_dir}/static")


# home URL
@app.route('/')
def index():
    # affichage de la page
    totalHours = es_object.total_hours(es_client)
    rows = es_object.es_search(es_client)
    page = formatPage(rows, totalHours)
    return make_response(render_template("page.html", page=page))


# search with user input
@app.route('/searchInput', methods=['POST'])
def searchInput():
    print("searchInput")
    print("User input : " + request.form['searchInput'])

    totalHours = es_object.total_hours(es_client)
    rows = es_object.search_on_name(es_client, request.form['searchInput'])
    if not rows:
        rows = es_object.es_search(es_client)

    page = formatPage(rows, totalHours)

    return make_response(render_template("page.html", page=page))


# Search with selected categories
@app.route('/searchCategory', methods=['POST'])
def searchCategory():
    print("searchCategory")
    print("User input : " + request.form['categorie'])

    totalHours = es_object.total_hours(es_client)
    rows = es_object.search_on_category(es_client, request.form['categorie'])
    if not rows:
        rows = es_object.es_search(es_client)

    page = formatPage(rows, totalHours)

    return make_response(render_template("page.html", page=page))


# search lessons with max hours lessons
@app.route('/max', methods=['POST'])
def searchMax():
    print("max")

    totalHours = es_object.total_hours(es_client)
    rows = es_object.list_max_hour(es_client)
    if not rows:
        rows = es_object.es_search(es_client)

    page = formatPage(rows, totalHours)

    return make_response(render_template("page.html", page=page))


# search lessons with min hours lessons
@app.route('/min', methods=['POST'])
def searchMin():
    print("min")

    totalHours = es_object.total_hours(es_client)
    rows = es_object.list_min_hour(es_client)
    if not rows:
        rows = es_object.es_search(es_client)

    page = formatPage(rows, totalHours)

    return make_response(render_template("page.html", page=page))


# main
if __name__ == '__main__':
    # Create Database
    es_object = ES(FILE_URL, INDEX_NAME, TYPE_NAME)
    data = es_object.load_data_from_file()
    es_client = es_object.create_es_client()
    es_object.create_es_index(es_client)
    es_object.insert_es_data(es_client, data)

    # Create Flask app
    app.config.update(ENV="development", DEBUG=True)
    app.run()
