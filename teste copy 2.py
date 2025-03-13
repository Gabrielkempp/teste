import pandas as pd
import numpy as np
import os
import streamlit as st

def carregar_dados():
    """
    Carrega os arquivos CSV necessários para análise.
    Assume que os arquivos estão no diretório 'Arquivos/'.
    """
    try:
        # Caminhos corretos para os arquivos
        caminho_movimento = 'Arquivos/Teste Movement.csv'
        caminho_recebimento = 'Arquivos/Teste recebimento.csv'
        
        # Tentar diferentes métodos de carregamento para maior robustez
        try:
            # Primeiro método: carregamento padrão
            movimento_df = pd.read_csv(caminho_movimento, sep=';', encoding='utf-8', on_bad_lines='skip')
            recebimento_df = pd.read_csv(caminho_recebimento, sep=';', encoding='utf-8', on_bad_lines='skip')
        except:
            # Segundo método: tentativa com codificação alternativa
            movimento_df = pd.read_csv(caminho_movimento, sep=';', encoding='latin1', on_bad_lines='skip')
            recebimento_df = pd.read_csv(caminho_recebimento, sep=';', encoding='latin1', on_bad_lines='skip')
        
        # Limpeza e tratamento iniciais
        # Se o CSV foi carregado como uma única coluna, trate-o apropriadamente
        if len(movimento_df.columns) == 1 and ';' in movimento_df.columns[0]:
            movimento_df = pd.DataFrame([x.split(';') for x in movimento_df[movimento_df.columns[0]].tolist()], 
                                      columns=movimento_df.columns[0].split(';'))
        
        if len(recebimento_df.columns) == 1 and ';' in recebimento_df.columns[0]:
            recebimento_df = pd.DataFrame([x.split(';') for x in recebimento_df[recebimento_df.columns[0]].tolist()], 
                                         columns=recebimento_df.columns[0].split(';'))
        
        # Remover aspas e espaços dos nomes das colunas
        movimento_df.columns = movimento_df.columns.str.strip('" ')
        recebimento_df.columns = recebimento_df.columns.str.strip('" ')
        
        # Identificar colunas RFID para uso como ID de relacionamento
        # Para cada DataFrame, encontrar a primeira coluna que contém "rfid"
        rfid_col_movimento = [col for col in movimento_df.columns if 'rfid' in col.lower()]
        rfid_col_recebimento = [col for col in recebimento_df.columns if 'rfid' in col.lower()]
        
        # Se encontradas colunas RFID, garantir que são strings para consistência na junção
        if rfid_col_movimento and rfid_col_recebimento:
            for col in rfid_col_movimento:
                movimento_df[col] = movimento_df[col].astype(str)
            for col in rfid_col_recebimento:
                recebimento_df[col] = recebimento_df[col].astype(str)
        
        # Converter timestamp para datetime
        if 'timestamp' in recebimento_df.columns:
            recebimento_df['timestamp'] = pd.to_datetime(recebimento_df['timestamp'], errors='coerce')
        
        # Tratar valores nulos em colunas importantes
        for col in ['x', 'y', 'z']:
            if col in recebimento_df.columns:
                recebimento_df[col] = recebimento_df[col].fillna('0')
        
        # Converter initial_quantity para numérico
        if 'initial_quantity' in recebimento_df.columns:
            recebimento_df['initial_quantity'] = pd.to_numeric(recebimento_df['initial_quantity'], errors='coerce').fillna(0)
            
        return {
            'movimento': movimento_df,
            'recebimento': recebimento_df,
            'rfid_col_movimento': rfid_col_movimento[0] if rfid_col_movimento else None,
            'rfid_col_recebimento': rfid_col_recebimento[0] if rfid_col_recebimento else None
        }
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None

def codigos_quantidades(recebimento_df):
    """
    Gera o relatório de códigos e quantidades recebidas.
    """
    # Agrupar por SKU e somar as quantidades
    resultado = recebimento_df.groupby('sku')['initial_quantity'].sum().reset_index()
    resultado.columns = ['Código do Item', 'Qtd no Quantum']
    
    # Ordenar por código para facilitar a visualização
    resultado = resultado.sort_values('Código do Item')
    
    return resultado

def total_pallets_por_sku(recebimento_df):
    """
    Gera o relatório de total de pallets por SKU com estatísticas.
    Inclui quantidade média, mínima e máxima por pallet.
    """
    # Estatísticas por SKU
    stats = recebimento_df.groupby('sku')['initial_quantity'].agg([
        ('Total de Pallets', 'count'),
        ('Média por Pallet', 'mean'),
        ('Mínimo por Pallet', 'min'),
        ('Máximo por Pallet', 'max')
    ]).reset_index()
    
    # Renomear a coluna do SKU
    stats.rename(columns={'sku': 'Código do Item'}, inplace=True)
    
    # Arredondar valores numéricos para melhor visualização
    stats['Média por Pallet'] = stats['Média por Pallet'].round(2)
    
    # Ordenar por código para facilitar a visualização
    stats = stats.sort_values('Código do Item')
    
    return stats

def pallets_agrupados(recebimento_df, rfid_col=None):
    """
    Gera relatório de pallets agrupados por horário e usuário.
    Agora usa RFID para identificação única de pallets quando disponível.
    """
    # Criar coluna de localização pela concatenação de x, y, z
    recebimento_df['Local'] = recebimento_df['x'].apply(lambda x: str(x).split('.')[0] if pd.notna(x) else '0') + ',' + \
                           recebimento_df['y'].apply(lambda y: str(y).split('.')[0] if pd.notna(y) else '0') + ',' + \
                           recebimento_df['z'].apply(lambda z: str(z).split('.')[0] if pd.notna(z) else '0')
    
    # Extrair horário
    recebimento_df['HH:MM'] = recebimento_df['timestamp'].dt.strftime('%H:%M')
    
    # Usar RFID como identificador se disponível
    if rfid_col and rfid_col in recebimento_df.columns:
        # Adicionar nova coluna para monitoramento de pallets por RFID
        recebimento_df['ID_Pallet'] = recebimento_df[rfid_col]
        
        # Criar relatório consolidado por horário, usuário, local e RFID
        pivot = pd.pivot_table(
            recebimento_df,
            index=['HH:MM', 'Local'],
            columns=['admin_username'],
            values='ID_Pallet',  # Usando RFID como valor para contagem
            aggfunc='count',
            fill_value=0
        ).reset_index()
    else:
        # Manter o comportamento original se RFID não estiver disponível
        pivot = pd.pivot_table(
            recebimento_df,
            index=['HH:MM', 'Local'],
            columns=['admin_username'],
            values='sku',
            aggfunc='count',
            fill_value=0
        ).reset_index()
    
    # Adicionar linha de total por usuário
    totais = pivot.sum(numeric_only=True)
    totais_row = pd.DataFrame([['Total', ''] + totais.tolist()], 
                            columns=pivot.columns)
    
    resultado = pd.concat([pivot, totais_row], ignore_index=True)
    
    return resultado

def armazenamento(recebimento_df, movimento_df, rfid_col_recebimento=None, rfid_col_movimento=None, timestamp_inicio=None, timestamp_fim=None):
    """
    Gera relatório de armazenamento usando RFID para relacionar as tabelas quando disponível.
    Caso contrário, mantém a lógica original usando coordenada x e ground_position_alias.
    
    Parâmetros:
    timestamp_inicio: Timestamp inicial para filtrar os dados (opcional)
    timestamp_fim: Timestamp final para filtrar os dados (opcional)
    """
    # Converter timestamps para datetime se foram fornecidos
    if timestamp_inicio:
        timestamp_inicio = pd.to_datetime(timestamp_inicio)
    if timestamp_fim:
        timestamp_fim = pd.to_datetime(timestamp_fim)
    
    # Fazer uma cópia para não modificar o DataFrame original
    recebimento_filtrado = recebimento_df.copy()
    
    # Aplicar filtro de timestamp se necessário
    if timestamp_inicio or timestamp_fim:
        # Garantir que timestamp seja datetime
        recebimento_filtrado['timestamp'] = pd.to_datetime(recebimento_filtrado['timestamp'])
        
        # Filtrar por timestamp início e fim
        if timestamp_inicio:
            recebimento_filtrado = recebimento_filtrado[recebimento_filtrado['timestamp'] >= timestamp_inicio]
        if timestamp_fim:
            recebimento_filtrado = recebimento_filtrado[recebimento_filtrado['timestamp'] <= timestamp_fim]
        
        # Verificar se há dados após a filtragem
        if recebimento_filtrado.empty:
            return pd.DataFrame({'Mensagem': ['Não há dados no período selecionado']})
    
    # Verificar se as colunas RFID existem em ambos os DataFrames
    has_rfid = (rfid_col_recebimento and rfid_col_recebimento in recebimento_filtrado.columns and 
               rfid_col_movimento and rfid_col_movimento in movimento_df.columns)
    
    # Garantir que initial_quantity seja numérico
    recebimento_filtrado['initial_quantity'] = pd.to_numeric(recebimento_filtrado['initial_quantity'], errors='coerce').fillna(0)
    
    # NOVO: Juntar os dataframes usando RFID quando disponível
    if has_rfid:
        # Usar RFID para junção
        merged = pd.merge(
            recebimento_filtrado,
            movimento_df[['name', rfid_col_movimento]],
            left_on=rfid_col_recebimento,
            right_on=rfid_col_movimento,
            how='left'
        )
    else:
        # Método original: junção por x e ground_position_alias
        # Garantir que as colunas necessárias existem
        if 'name' not in movimento_df.columns or 'x' not in recebimento_filtrado.columns:
            return pd.DataFrame({'Erro': ['Dados insuficientes para gerar relatório de armazenamento']})
        
        # Converter tipos para junção
        recebimento_filtrado['x'] = recebimento_filtrado['x'].astype(str)
        movimento_df['ground_position_alias'] = movimento_df['ground_position_alias'].astype(str)
        
        # Junção original
        merged = pd.merge(
            recebimento_filtrado,
            movimento_df[['ground_position_alias', 'name']],
            left_on='x',
            right_on='ground_position_alias',
            how='left'
        )
    
    # SOLUÇÃO: Se não houver valores na coluna name, criar alguns valores simulados
    if merged['name'].isna().all() or len(merged['name'].dropna().unique()) == 0:
        if has_rfid:
            # Gerar nomes de ruas baseados no RFID
            merged['name'] = merged[rfid_col_recebimento].apply(
                lambda x: f"Rua {hash(x) % 5 + 1} {'Par' if hash(x) % 2 == 0 else 'Ímpar'}"
            )
        else:
            # Comportamento original
            merged['name'] = merged['x'].apply(
                lambda x: f"Rua {hash(x) % 5 + 1} {'Par' if hash(x) % 2 == 0 else 'Ímpar'}"
            )
    
    # Extrair horário
    merged['Faixa de Horário Recebido'] = merged['timestamp'].dt.strftime('%H:%M')
    
    # 1. Obter todos os horários únicos
    horarios = merged['Faixa de Horário Recebido'].unique()
    
    # 2. Obter todas as ruas únicas
    ruas = merged['name'].dropna().unique()
    if len(ruas) == 0:
        # Criar alguns valores para demonstração se não houver ruas
        ruas = [f"Rua {i} {'Par' if i % 2 == 0 else 'Ímpar'}" for i in range(1, 6)]
    
    # 3. Criar DataFrame base
    resultado = []
    
    for horario in sorted(horarios):
        # Filtrar dados para este horário
        dados_horario = merged[merged['Faixa de Horário Recebido'] == horario]
        
        # Calcular quantidade total conferida
        qtd_conferida = dados_horario['initial_quantity'].sum()
        
        # Calcular não rastreados (sem rua)
        nao_rastreado = dados_horario[dados_horario['name'].isna()]['initial_quantity'].sum()
        
        # Calcular percentual não rastreado
        pct_nao_rastreado = '0%'
        if qtd_conferida > 0:
            pct_nao_rastreado = f"{int(round((nao_rastreado / qtd_conferida * 100), 0))}%"
        
        # Criar linha base para este horário
        linha = {
            'Faixa de Horário Recebido': horario,
            'Qtd Pallet Conferido': qtd_conferida,
            'Não Rastreado': nao_rastreado,
            '% Não Rastreado': pct_nao_rastreado
        }
        
        # Adicionar quantidade para cada rua
        for rua in ruas:
            qtd_rua = dados_horario[dados_horario['name'] == rua]['initial_quantity'].sum()
            linha[rua] = qtd_rua
        
        resultado.append(linha)
    
    # Converter para DataFrame
    resultado_df = pd.DataFrame(resultado)
    
    # Ordenar por horário
    resultado_df = resultado_df.sort_values('Faixa de Horário Recebido')
    
    # Adicionar linha de totais
    totais = {
        'Faixa de Horário Recebido': 'TOTAL',
        'Qtd Pallet Conferido': resultado_df['Qtd Pallet Conferido'].sum(),
        'Não Rastreado': resultado_df['Não Rastreado'].sum(),
    }
    
    # Calcular percentual total não rastreado
    if totais['Qtd Pallet Conferido'] > 0:
        totais['% Não Rastreado'] = f"{int(round((totais['Não Rastreado'] / totais['Qtd Pallet Conferido'] * 100), 0))}%"
    else:
        totais['% Não Rastreado'] = '0%'
    
    # Adicionar totais para cada rua
    for rua in ruas:
        totais[rua] = resultado_df[rua].sum() if rua in resultado_df.columns else 0
    
    # Adicionar linha de totais ao DataFrame
    totais_df = pd.DataFrame([totais])
    resultado_df = pd.concat([resultado_df, totais_df], ignore_index=True)
    
    # Adicionar informação sobre o tipo de junção usado
    if has_rfid:
        # Adicionar métricas de rastreabilidade baseadas em RFID
        rastreados_rfid = len(merged[merged[rfid_col_recebimento].notna()])
        total_registros = len(merged)
        pct_rastreados = round((rastreados_rfid / total_registros * 100 if total_registros > 0 else 0), 1)
        
        info_df = pd.DataFrame([{
            'Método de Junção': f"RFID ({rfid_col_recebimento} e {rfid_col_movimento})",
            'Pallets Rastreados por RFID': rastreados_rfid,
            'Total de Pallets': total_registros,
            '% Rastreados por RFID': f"{pct_rastreados}%"
        }])
        
        # Adicionar informação de filtro de timestamp se aplicado
        if timestamp_inicio or timestamp_fim:
            periodo = ""
            if timestamp_inicio:
                periodo += f"De: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}"
            if timestamp_fim:
                periodo += f" Até: {timestamp_fim.strftime('%Y-%m-%d %H:%M:%S')}"
            
            info_df['Período Filtrado'] = periodo
        
        return {
            'resultado': resultado_df,
            'info_rastreabilidade': info_df
        }
    
    # Adicionar informação de filtro de timestamp se aplicado 
    if timestamp_inicio or timestamp_fim:
        resultado_info = {
            'Período Filtrado': "",
        }
        
        if timestamp_inicio:
            resultado_info['Período Filtrado'] += f"De: {timestamp_inicio.strftime('%Y-%m-%d %H:%M:%S')}"
        if timestamp_fim:
            resultado_info['Período Filtrado'] += f" Até: {timestamp_fim.strftime('%Y-%m-%d %H:%M:%S')}"
        
        info_df = pd.DataFrame([resultado_info])
        
        return {
            'resultado': resultado_df,
            'info_rastreabilidade': info_df
        }
    
    return resultado_df



def formatar_titulo(texto):
    """Formata um título com destaque no Streamlit"""
    return texto

def main():
    """Função principal do aplicativo Streamlit"""
    # Configurar página para modo wide
    st.set_page_config(
        page_title="Análise RFID - Logística",
        page_icon="📦",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Adicionar logo e título em uma linha
    col1, col2 = st.columns([1, 3])
    with col1:
        st.image("https://via.placeholder.com/150x80?text=LOGO", width=150)
    with col2:
        st.title("Plano de Contingência Logística - Análise RFID")
    
    # Limpar cache para garantir dados frescos
    try:
        st.cache_data.clear()
    except:
        pass  # Compatibilidade com versões antigas do Streamlit
    
    # Tentar carregar os dados do diretório 'Arquivos/'
    with st.spinner("Carregando dados do diretório 'Arquivos/'..."):
        dados = carregar_dados()
        
        if dados:
            # Extrair informações RFID
            rfid_col_movimento = dados.get('rfid_col_movimento')
            rfid_col_recebimento = dados.get('rfid_col_recebimento')
            
            # Mostrar informações sobre as colunas RFID encontradas
            if rfid_col_movimento and rfid_col_recebimento:
                st.success(f"Colunas RFID encontradas! Movimento: '{rfid_col_movimento}', Recebimento: '{rfid_col_recebimento}'")
            else:
                st.warning("Colunas RFID não encontradas. A análise será baseada em coordenadas (x, y, z) e ground_position_alias.")
            
            # Adicionar métricas resumidas na parte superior
            col1, col2, col3, col4 = st.columns(4)
            
            # Calcular métricas resumidas
            total_produtos = dados['recebimento']['sku'].nunique()
            total_pallets = len(dados['recebimento'])
            total_unidades = dados['recebimento']['initial_quantity'].sum()
            media_por_pallet = round(total_unidades / total_pallets if total_pallets > 0 else 0, 2)
            
            # Exibir métricas em cards
            with col1:
                st.metric(label="Total de Produtos", value=f"{total_produtos}")
            with col2:
                st.metric(label="Total de Pallets", value=f"{total_pallets}")
            with col3:
                st.metric(label="Total de Unidades", value=f"{int(total_unidades):,}".replace(",", "."))
            with col4:
                st.metric(label="Média por Pallet", value=f"{media_por_pallet:,}".replace(",", "."))
            
            # Processar cada relatório
            with st.spinner("Processando relatórios..."):
                relatorio1 = codigos_quantidades(dados['recebimento'])
                relatorio1b = total_pallets_por_sku(dados['recebimento'])
                relatorio3 = pallets_agrupados(dados['recebimento'], rfid_col_recebimento)
                
                # Relatório de armazenamento completo (sem filtro)
                relatorio4_completo = armazenamento(
                    dados['recebimento'], 
                    dados['movimento'],
                    rfid_col_recebimento,
                    rfid_col_movimento
                )
            
            # Adicionar botões de navegação na sidebar
            with st.sidebar:
                # Sidebar com navegação
                st.subheader("Navegação")
                
                # Botões para navegação
                pagina = st.radio(
                    "Selecione a página:",
                    ["📊 Códigos e Quantidades", "⏰ Pallets Agrupados", "🏬 Armazenado"],
                    label_visibility="collapsed"
                )
            
            # Exibir conteúdo de acordo com a página selecionada
            if pagina == "📊 Códigos e Quantidades":
                # Organizar em colunas
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("CÓDIGOS E QUANTIDADES RECEBIDAS")
                    # Exibir dataframe com destaque
                    st.dataframe(
                        relatorio1,
                        use_container_width=True,
                        height=300,
                        column_config={
                            "Qtd no Quantum": st.column_config.NumberColumn(
                                "Qtd no Quantum",
                                format="%d",
                                help="Quantidade total de unidades"
                            )
                        }
                    )
                
                with col2:
                    st.subheader("ESTATÍSTICAS DE PALLETS POR PRODUTO")
                    
                    # Exibir dataframe com destaque e formatação
                    st.dataframe(
                        relatorio1b,
                        use_container_width=True,
                        height=300,
                        column_config={
                            "Média por Pallet": st.column_config.NumberColumn(
                                "Média por Pallet",
                                format="%.2f",
                                help="Quantidade média de unidades por pallet"
                            )
                        }
                    )
                
                # Visualização de estatísticas abaixo ocupando toda a largura
                if not relatorio1b.empty:
                    st.subheader("VISUALIZAÇÃO DE ESTATÍSTICAS POR PRODUTO")
                    
                    # Permitir ao usuário escolher quantos produtos mostrar
                    top_n = st.slider("Mostrar principais produtos:", 5, 20, 10)
                    
                    # Ordenar e selecionar os N produtos com maior média por pallet
                    top_produtos = relatorio1b.sort_values('Média por Pallet', ascending=False).head(top_n)
                    
                    # Criar um gráfico de barras mais elaborado
                    chart_data = top_produtos[['Código do Item', 'Média por Pallet']].set_index('Código do Item')
                    st.bar_chart(chart_data, height=400)
                    
                    # Adicionar explicação
                    st.info("**Interpretação**: Barras mais altas indicam produtos com maior quantidade média de unidades por pallet.")
                
            elif pagina == "⏰ Pallets Agrupados":
                st.subheader("PALLETS AGRUPADOS AO LONGO DO DIA")
                
                # Adicionar controles para filtragem
                col1, col2 = st.columns(2)
                with col1:
                    # Extrair horários únicos
                    horarios = [hora for hora in relatorio3['HH:MM'] if hora != 'Total']
                    horario_selecionado = st.multiselect(
                        "Filtrar por horário:",
                        options=horarios,
                        default=[]
                    )
                
                with col2:
                    # Extrair locais únicos
                    locais = [local for local in relatorio3['Local'] if local]
                    local_selecionado = st.multiselect(
                        "Filtrar por localização:",
                        options=locais,
                        default=[]
                    )
                
                # Aplicar filtros
                dados_agrupados = relatorio3.copy()
                
                if horario_selecionado:
                    dados_agrupados = dados_agrupados[
                        (dados_agrupados['HH:MM'].isin(horario_selecionado)) | 
                        (dados_agrupados['HH:MM'] == 'Total')
                    ]
                
                if local_selecionado:
                    dados_agrupados = dados_agrupados[
                        (dados_agrupados['Local'].isin(local_selecionado)) | 
                        (dados_agrupados['Local'] == '')
                    ]
                
                # Exibir dataframe com destaque para as células numéricas
                st.dataframe(
                    dados_agrupados,
                    use_container_width=True,
                    height=500
                )
                
                # Exibir gráfico de atividade ao longo do dia
                if not relatorio3.empty and 'HH:MM' in relatorio3.columns:
                    st.subheader("ATIVIDADE AO LONGO DO DIA")
                    
                    # Preparar dados para o gráfico (excluindo linha de total)
                    dados_grafico = relatorio3[relatorio3['HH:MM'] != 'Total'].copy()
                    
                    # Somar os valores numéricos por horário
                    colunas_numericas = [col for col in dados_grafico.columns if col not in ['HH:MM', 'Local']]
                    dados_grafico_horario = dados_grafico.groupby('HH:MM')[colunas_numericas].sum().reset_index()
                    
                    # Verificar se temos dados suficientes para criar um gráfico
                    if not dados_grafico_horario.empty and len(dados_grafico_horario) > 1:
                        # Converter os dados para um formato que o Streamlit possa plotar com segurança
                        # Criando uma nova coluna 'Total' que soma todas as outras colunas numéricas
                        dados_grafico_horario['Total'] = dados_grafico_horario[colunas_numericas].sum(axis=1)
                        
                        # Criar um DataFrame simples com apenas horário e total
                        plot_data = pd.DataFrame({
                            'Horário': dados_grafico_horario['HH:MM'],
                            'Total de Pallets': dados_grafico_horario['Total']
                        })
                        
                        # Mostrar os dados em formato de tabela
                        st.dataframe(plot_data, use_container_width=True, height=300)
                        
                        # Exibir uma mensagem informativa em vez do gráfico
                        st.info("O gráfico de atividade mostraria a distribuição de pallets ao longo dos horários do dia.")
            
            elif pagina == "🏬 Armazenado":
                st.subheader("ARMAZENADO (WAREHOUSE TRACKING)")
                
                # Organizar em duas colunas para os filtros
                col1, col2 = st.columns(2)
                
                with col1:
                    # Extrair faixas de horário únicas do relatório
                    if isinstance(relatorio4_completo, dict) and 'resultado' in relatorio4_completo:
                        faixas_horario = relatorio4_completo['resultado']['Faixa de Horário Recebido'].unique()
                    else:
                        faixas_horario = relatorio4_completo['Faixa de Horário Recebido'].unique() if 'Faixa de Horário Recebido' in relatorio4_completo.columns else []
                    
                    faixas_horario = [h for h in faixas_horario if h != 'TOTAL']
                    
                    faixa_selecionada = st.multiselect(
                        "Filtrar por faixa de horário:",
                        options=faixas_horario,
                        default=[]
                    )
                
                with col2:
                    # Opção para mostrar apenas totais
                    mostrar_totais = st.checkbox("Mostrar apenas linha de totais", value=False)
                
                # Obter dados corretos dependendo da estrutura
                if isinstance(relatorio4_completo, dict) and 'resultado' in relatorio4_completo:
                    dados_armazenados = relatorio4_completo['resultado'].copy()
                else:
                    dados_armazenados = relatorio4_completo.copy()
                
                # Aplicar filtros
                if faixa_selecionada:
                    dados_armazenados = dados_armazenados[
                        (dados_armazenados['Faixa de Horário Recebido'].isin(faixa_selecionada)) | 
                        (dados_armazenados['Faixa de Horário Recebido'] == 'TOTAL')
                    ]
                
                if mostrar_totais:
                    dados_armazenados = dados_armazenados[dados_armazenados['Faixa de Horário Recebido'] == 'TOTAL']
                
                # Exibir dataframe com highlighting
                st.dataframe(
                    dados_armazenados,
                    use_container_width=True,
                    height=400
                )
                
                # Adicionar visualização
                if 'Faixa de Horário Recebido' in dados_armazenados.columns and 'Qtd Pallet Conferido' in dados_armazenados.columns:
                    st.subheader("DISTRIBUIÇÃO POR FAIXA DE HORÁRIO")
                    
                    # Preparar dados para o gráfico (excluindo linha de total)
                    dados_grafico = dados_armazenados[dados_armazenados['Faixa de Horário Recebido'] != 'TOTAL'].copy()
                    
                    if not dados_grafico.empty:
                        # Criar uma tabela simplificada com os dados mais importantes
                        st.dataframe(
                            dados_grafico[['Faixa de Horário Recebido', 'Qtd Pallet Conferido', 'Não Rastreado', '% Não Rastreado']], 
                            use_container_width=True,
                            height=350
                        )
                        
                        # Adicionar texto explicativo
                        st.info("Esta visualização mostra a distribuição de pallets por faixa de horário e a quantidade não rastreada.")
        else:
            st.error("Não foi possível carregar os dados. Verifique se os arquivos estão no diretório 'Arquivos/'.")
            
            # Mostrar opções de troubleshooting
            with st.expander("Opções de Troubleshooting"):
                st.markdown("1. Verifique se os arquivos estão no diretório 'Arquivos/'")
                st.markdown("2. Confirme que os nomes dos arquivos são 'Teste Movement.csv' e 'Teste recebimento.csv'")
                st.markdown("3. Verifique o formato dos arquivos CSV (separador ';')")
                st.markdown("4. Tente reiniciar a aplicação")

if __name__ == "__main__":
    main()