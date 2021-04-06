from flask_table import Table, Col


class ItemTable(Table):
    nom = Col('Matière')
    categorie = Col('Catégorie')
    nb_heure = Col("Nombre d'heures")
    date_pre_cours = Col('Premier cours')
    date_der_cours = Col('Dernier cours')
    description = Col('Description')


def formatPage(rows, totalHours, categories):

    table = ItemTable(rows, classes=['tableau'], border=1)
    listCategory = []
    for cat in categories:
        listCategory.append({'name': cat})
    print(listCategory)
    page = {
        "totalHours": totalHours,
        "table": table,
        "categories": listCategory
    }
    return page
