"""API HTTP ``query()`` (commande ``myquantstore serve``).

Expose l'historique agrégé en HTTP (Parquet / Arrow IPC) pour un client
quelconque — autre langage, autre machine, notebook — **sans** partager
``data_dir`` ni importer le package.

Ce n'est **pas** le serveur chart (``/api/candles``) et **pas** un
remplacement du snapshot hebdo. Aucune cascade / fetch réseau.

Voir :mod:`myquantstore.serve.server` et ``docs/TODO_SERVE.md``.
"""
