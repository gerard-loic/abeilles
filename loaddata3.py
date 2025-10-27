"""
Script pour générer la liste des URLs de fichiers Parquet
pour les données météo quotidiennes du département 67 (1990-2020)

Source: https://www.data.gouv.fr/fr/datasets/parquet-donnees-climatologiques-de-base-mensuelles-quotidiennes-horaires-6-minutes-format-parquet/
Auteur du dataset Parquet: Maxime Pawlak (Mister Meteo)
"""

def generer_urls_parquet_quotidien(annee_debut=1990, annee_fin=2020):
    """
    Génère la liste des URLs de fichiers Parquet contenant les données quotidiennes
    pour la période spécifiée.
    
    Note: Les fichiers Parquet contiennent TOUS les départements dans un seul fichier.
    Vous devrez filtrer par département (67) après téléchargement.
    
    Paramètres:
    -----------
    annee_debut : int
        Année de début (par défaut: 1990)
    annee_fin : int
        Année de fin (par défaut: 2020)
    
    Retourne:
    ---------
    list : Liste des URLs de fichiers Parquet à télécharger
    """
    
    base_url = "https://object.files.data.gouv.fr/meteofrance-mistermeteo/"
    
    # Structure des fichiers Parquet selon la documentation
    # Format: quotidien.{periode}.parquet ou quotidien.{periode}.prepared.parquet
    
    # Définition des périodes disponibles basée sur la structure typique
    # Ces périodes sont généralement regroupées par tranches
    periodes = []
    
    # Périodes historiques (regroupées par décennies ou plus)
    if annee_debut <= 1949:
        periodes.append("previous-1950")
    
    if annee_debut <= 1989 and annee_fin >= 1950:
        periodes.append("1950-1959")
        periodes.append("1960-1969")
        periodes.append("1970-1979")
        periodes.append("1980-1989")
    
    if annee_debut <= 2021 and annee_fin >= 1990:
        periodes.append("1990-1999")
        periodes.append("2000-2009")
        periodes.append("2010-2019")
        periodes.append("2020-2021")
    
    # Période récente (mise à jour fréquemment)
    if annee_fin >= 2022:
        periodes.append("latest-2022-2023")
        periodes.append("latest-2023-2024")
        periodes.append("latest-2024-2025")
    
    # Générer les URLs
    urls = []
    urls_prepared = []
    
    for periode in periodes:
        # URL du fichier brut
        url_brut = f"{base_url}quotidien.{periode}.parquet"
        urls.append(url_brut)
        
        # URL du fichier préparé (.prepared)
        url_prepared = f"{base_url}quotidien.{periode}.prepared.parquet"
        urls_prepared.append(url_prepared)
    
    return urls, urls_prepared


def afficher_urls(annee_debut=1990, annee_fin=2020, format_sortie="texte"):
    """
    Affiche les URLs des fichiers Parquet nécessaires.
    
    Paramètres:
    -----------
    annee_debut : int
        Année de début
    annee_fin : int
        Année de fin
    format_sortie : str
        Format de sortie: "texte", "json", "python"
    """
    
    urls_brut, urls_prepared = generer_urls_parquet_quotidien(annee_debut, annee_fin)
    
    print("=" * 80)
    print(f"URLs des fichiers Parquet pour les données quotidiennes {annee_debut}-{annee_fin}")
    print("=" * 80)
    print()
    
    print("IMPORTANT:")
    print("- Ces fichiers contiennent les données pour TOUS les départements")
    print("- Vous devrez filtrer les données pour le département 67 après téléchargement")
    print("- Les fichiers '.prepared' sont recommandés (colonnes nettoyées et typées)")
    print()
    
    if format_sortie == "texte":
        print("-" * 80)
        print("FICHIERS PRÉPARÉS (recommandés):")
        print("-" * 80)
        for i, url in enumerate(urls_prepared, 1):
            print(f"{i}. {url}")
        
        print()
        print("-" * 80)
        print("FICHIERS BRUTS (alternatifs):")
        print("-" * 80)
        for i, url in enumerate(urls_brut, 1):
            print(f"{i}. {url}")
    
    elif format_sortie == "json":
        import json
        data = {
            "periode": f"{annee_debut}-{annee_fin}",
            "fichiers_prepares": urls_prepared,
            "fichiers_bruts": urls_brut,
            "note": "Filtrer par département 67 après téléchargement"
        }
        print(json.dumps(data, indent=2, ensure_ascii=False))
    
    elif format_sortie == "python":
        print("# Liste des URLs (fichiers préparés)")
        print("urls_prepared = [")
        for url in urls_prepared:
            print(f'    "{url}",')
        print("]")
        print()
        print("# Liste des URLs (fichiers bruts)")
        print("urls_brut = [")
        for url in urls_brut:
            print(f'    "{url}",')
        print("]")
    
    print()
    print("=" * 80)
    print("COMMENT UTILISER CES FICHIERS:")
    print("=" * 80)
    print("""
1. Télécharger les fichiers Parquet:
   
   import requests
   for url in urls_prepared:
       nom_fichier = url.split('/')[-1]
       response = requests.get(url)
       with open(nom_fichier, 'wb') as f:
           f.write(response.content)

2. Lire et filtrer les données (requiert: pip install pandas pyarrow):
   
   import pandas as pd
   
   # Lire un fichier Parquet
   df = pd.read_parquet('quotidien.1990-1999.prepared.parquet')
   
   # Filtrer pour le département 67
   # (La colonne exacte peut varier: 'NUM_POSTE', 'DEPT', etc.)
   df_67 = df[df['NUM_POSTE'].str.startswith('67')]
   
   # Ou si la colonne département existe:
   # df_67 = df[df['DEPT'] == '67']
   
   # Filtrer par période (1990-2020)
   df_67 = df_67[(df_67['AAAAMMJJ'] >= 19900101) & (df_67['AAAAMMJJ'] <= 20201231)]
   
3. Combiner plusieurs fichiers:
   
   import pandas as pd
   import glob
   
   # Lire tous les fichiers Parquet
   fichiers = glob.glob('quotidien.*.prepared.parquet')
   
   dfs = []
   for fichier in fichiers:
       df = pd.read_parquet(fichier)
       # Filtrer pour le département 67
       df_67 = df[df['NUM_POSTE'].str.startswith('67')]
       dfs.append(df_67)
   
   # Combiner tous les DataFrames
   df_final = pd.concat(dfs, ignore_index=True)
   
   # Trier par date
   df_final = df_final.sort_values('AAAAMMJJ')
   
   # Sauvegarder
   df_final.to_parquet('meteo_dep67_1990_2020.parquet')
   df_final.to_csv('meteo_dep67_1990_2020.csv', index=False)
""")
    
    print("=" * 80)
    print("INFORMATIONS SUPPLÉMENTAIRES:")
    print("=" * 80)
    print(f"""
- Source: data.gouv.fr (Maxime Pawlak - Mister Meteo)
- Dataset: https://www.data.gouv.fr/fr/datasets/parquet-donnees-climatologiques-de-base-mensuelles-quotidiennes-horaires-6-minutes-format-parquet/
- Licence: Licence Ouverte 2.0
- Format: Parquet (optimisé, compressé)
- Contenu: Toutes les stations de tous les départements
- Mise à jour: Hebdomadaire pour les données récentes
- Contact dataset: contact@mistermeteo.com
""")


def sauvegarder_urls_fichier(annee_debut=1990, annee_fin=2020, nom_fichier="urls_parquet.txt"):
    """
    Sauvegarde les URLs dans un fichier texte.
    """
    urls_brut, urls_prepared = generer_urls_parquet_quotidien(annee_debut, annee_fin)
    
    with open(nom_fichier, 'w', encoding='utf-8') as f:
        f.write(f"# URLs Parquet - Données météo quotidiennes {annee_debut}-{annee_fin}\n")
        f.write(f"# Département 67 (Bas-Rhin)\n")
        f.write(f"# Note: Ces fichiers contiennent tous les départements\n\n")
        
        f.write("# FICHIERS PRÉPARÉS (recommandés)\n")
        for url in urls_prepared:
            f.write(f"{url}\n")
        
        f.write("\n# FICHIERS BRUTS (alternatifs)\n")
        for url in urls_brut:
            f.write(f"{url}\n")
    
    print(f"✓ URLs sauvegardées dans: {nom_fichier}")


if __name__ == "__main__":
    import sys
    
    # Paramètres par défaut
    annee_debut = 1950
    annee_fin = 2020
    format_sortie = "texte"
    
    # Gestion des arguments en ligne de commande
    if len(sys.argv) > 1:
        if sys.argv[1] == "--json":
            format_sortie = "json"
        elif sys.argv[1] == "--python":
            format_sortie = "python"
        elif sys.argv[1] == "--save":
            sauvegarder_urls_fichier(annee_debut, annee_fin)
            sys.exit(0)
        elif sys.argv[1] == "--help":
            print("Usage: python lister_urls_parquet.py [OPTIONS]")
            print()
            print("Options:")
            print("  --json     Afficher les URLs au format JSON")
            print("  --python   Afficher les URLs comme liste Python")
            print("  --save     Sauvegarder les URLs dans un fichier texte")
            print("  --help     Afficher cette aide")
            sys.exit(0)
    
    # Afficher les URLs
    afficher_urls(annee_debut, annee_fin, format_sortie)