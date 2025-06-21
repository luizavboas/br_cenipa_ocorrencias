
  # Dados CENIPA de Ocorrências Aeronáuticas  
  Este é um repositório que extrai e trata dados retirados do Portal Dados Abertos
  
  ## Estrutura 
  ### Crawler
  A extração dos dados pode ser feita por meio de dois métodos:
  1. Webscraping tradicional, usando selenium:
    a. Acesso à página inicial do conjunto no Portal de Dados Abertos;
    b. Busca pela seta de drop down "Recursos" e click;
    c. Para cada recurso: busca pelo botão "Acessar o recurso" e click;

  2. API do Portal de Dados Abertos:
    a. Acesso a https://dados.gov.br/swagger-ui/index.html;
    
  ## Rodando Localmente  

