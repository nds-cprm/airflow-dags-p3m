import subprocess
import logging
import os

task_logger = logging.getLogger("airflow.task")

#Função que cria o link simbólico em caso de execução da branch_b, e direciona o backup da pasta da execução atual para da ultima excução com base igual
def simbolic_link(ti):
    a_path=ti.xcom_pull(key='a_path')#caminho do diretorio de backup da execução atual
    p_path=ti.xcom_pull(key='p_path')#caminho do diretorio de backup da base correspondente igual a atual
    lista = ti.xcom_pull(key='lista')
    
    #Em caso de falta de atualização recorrente a ultima execução anterior também pode ter gerado um link logo, o bloco itera os itens da pasta e verifica a existência de um link
    #em caso da existência de um link o link atual faz referência ao link anterior para se construir o lastro das bases
   
    itens=os.listdir(p_path)
    task_logger.info(a_path)
    task_logger.info(p_path)
    task_logger.info(lista)
    task_logger.info(itens)

    task_logger.info('p_path', p_path)

    l = []
    for i in itens:
        nome = i.split('/')[-1]
        if not nome in ['sha256', 'txt']:
            task_logger.info(i)
            task_logger.info('1')
            if any(i in item for item in lista):
                link = os.path.join(p_path,i)
                task_logger.info(link)
                task_logger.info('2')
                if os.path.islink(link):
                    l=1
                    task_logger.info(f'Removendo {a_path}/{nome}')
                    result = subprocess.run(
                                    "rm " f'{a_path}/{nome}',
                                    shell=True,text=True, capture_output=True)
                    
                    if result.returncode != 0:
                        task_logger.info('result != 0')
                        task_logger.error(result.stderr)
                        exit -1 #type:ignore
    if l ==1:
        try:
            subprocess.run("ln -s " +link+" " +f'{a_path}/redirec_base.txt', shell=True,
                text=True, capture_output = True)
            return 0
        except:
            print('except')   
        
    #Em caso padrão, execução anterior possui uma base, criação do link para o arquivo gdb.zip    
    for n in lista:
        nome = n.split('/')[-1]
        task_logger.info(f'{p_path}'+'/' +f'{a_path}/redirec_base.txt')
        result = subprocess.run("rm " f'{a_path}/{nome}',
                                shell=True,text=True, capture_output=True)
        if result.returncode != 0:
            task_logger.info('result != 0 2!')
            task_logger.error(result.stderr)
    subprocess.run("ln -s "
                                +p_path+"/"+nome
                                +f' {a_path}/redirec_base.txt', shell=True, text = True, capture_output=True)

    task_logger.info('Como não houve atualização da base desde a ultima execução, a execução do dia atual possui os dados equivalentes da anterior')
    task_logger.info('Para otimizar o sistema de backup a base atual não será duplicada, foi criado um link simbólico direcionando para base '+p_path)