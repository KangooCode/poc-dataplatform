import csv
import time
from datetime import date
from faker import Faker

# --- Configuration ---
OUTPUT_FILE  = "fake_data.csv"
TOTAL_ROWS   = 5_000_000
CHUNK_SIZE   = 10_000        # Nombre de lignes générées par batch
LOCALE       = "fr_FR"       # Locale française pour des données réalistes

# Faker est thread-safe mais pas multiprocess.
# On l'instancie une seule fois : c'est le principal levier de perf.
fake = Faker(LOCALE)

# Fixer le seed rend la génération reproductible (utile pour les tests).
# Retire cette ligne si tu veux des données différentes à chaque run.
Faker.seed(42)


def generate_chunk(size: int, start_id: int) -> list[list]:
    """
    Génère un batch de `size` lignes.
    Retourne une liste de listes (plus rapide que des dicts pour csv.writer).
    """
    rows = []
    for i in range(size):
        nom    = fake.last_name()
        prenom = fake.first_name()

        # fake.address() renvoie une adresse multiligne avec \n.
        # On remplace les sauts de ligne pour éviter de casser le CSV.
        adresse = fake.address().replace("\n", ", ")

        # Date entre 1940 et 2005 pour des profils réalistes
        date_naissance = fake.date_of_birth(
            minimum_age=19,
            maximum_age=84
        ).strftime("%Y-%m-%d")

        # Email construit à partir du nom/prénom pour plus de cohérence,
        # avec un domaine français aléatoire.
        email = fake.email()

        rows.append([start_id + i, nom, prenom, adresse, date_naissance, email])
    return rows


def main():
    start = time.time()
    rows_written = 0

    # newline="" est requis par le module csv sur tous les OS
    # pour éviter les doubles sauts de ligne sous Windows.
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)

        # En-tête
        writer.writerow(["id", "nom", "prenom", "adresse_postale", "date_naissance", "adresse_mail"])

        # Génération et écriture par chunks
        while rows_written < TOTAL_ROWS:
            # Le dernier chunk peut être plus petit que CHUNK_SIZE
            current_chunk_size = min(CHUNK_SIZE, TOTAL_ROWS - rows_written)
            chunk = generate_chunk(current_chunk_size, start_id=rows_written + 1)
            writer.writerows(chunk)
            rows_written += current_chunk_size

            # Affichage de la progression tous les 500 000 lignes
            if rows_written % 500_000 == 0:
                elapsed = time.time() - start
                pct     = rows_written / TOTAL_ROWS * 100
                speed   = rows_written / elapsed
                print(
                    f"  {rows_written:>9,} lignes  "
                    f"({pct:5.1f}%)  —  "
                    f"{speed:,.0f} lignes/sec  —  "
                    f"{elapsed:.1f}s écoulées"
                )

    total_time = time.time() - start
    print(f"\n✓ Fichier généré : {OUTPUT_FILE}")
    print(f"  {TOTAL_ROWS:,} lignes en {total_time:.1f}s "
          f"({TOTAL_ROWS / total_time:,.0f} lignes/sec)")


if __name__ == "__main__":
    print(f"Génération de {TOTAL_ROWS:,} lignes → {OUTPUT_FILE}\n")
    main()