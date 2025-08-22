"""
Pacote `src` do projeto "ifood-case-cupons".

Contém módulos para ETL, análise A/B e por segmentos, visualizações e utilitários
usados no case técnico de análise de cupons.

Principais módulos:
 - `etl`: funções para leitura, conformação e limpeza dos dados brutos.
 - `analysis_ab`: funções de resumo A/B, métricas robustas e testes estatísticos.
 - `analysis_segments`: agregações e transformações por segmentos.
 - `viz_ab`, `viz_segments`: funções de plotagem e salvamento de figuras.
 - `utils`, `checks`, `finance`: utilitários, checagens e cálculos financeiros.

Este arquivo apenas documenta o pacote — as funções e classes são exportadas
pelos seus respectivos módulos.
"""

__all__ = [
	"etl",
	"analysis_ab",
	"analysis_segments",
	"viz_ab",
	"viz_segments",
	"utils",
	"checks",
	"finance",
]
