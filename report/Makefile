.PHONY: all clean

all:
	mkdir -p build
	podman run --rm -v "$(CURDIR):/work:Z" -w /work docker.io/texlive/texlive:latest \
		pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build report.tex
	podman run --rm -v "$(CURDIR):/work:Z" -w /work docker.io/texlive/texlive:latest \
		pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build report.tex
	cp build/report.pdf ../Rapport_Bilal_Ziyane_Entrepot_Temps_Reel.pdf

clean:
	rm -rf build
