# -*- coding: utf-8 -*-
"""
Created on Thu Oct 28 23:39:48 2021

@author: mc_ka
"""

import pandas as pd
import psycopg2
import json
import urllib.request as urllib2
import plotly.graph_objects as go
import plotly.express as px
import os

class connect:
    def __init__(self):
        f = open(os.path.join(os.getcwd(),"setup.json"))
        setup_config = json.load(f)
        
        self.host=setup_config["host"]
        self.database=setup_config["db"]
        self.usr=setup_config["usr"]
        self.pwd=setup_config["pwd"]
    
    def connectdb(self):
        self.conn=psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    user=self.usr,
                    password=self.pwd)
        self.cursor=self.conn.cursor()
    
    def disconnectdb(self):
        self.cursor.close()
        self.conn.close()
        
    def getversion(self):
        #initiate
        self.connectdb()
	# execute a statement
        print('PostgreSQL database version:')
        self.cursor.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = self.cursor.fetchone()
        print(db_version)
        #cleanup
        self.disconnectdb()
    
    def gettables(self):
        #initiate
        self.connectdb()        
        
        data = pd.read_sql("SELECT * FROM pg_catalog.pg_tables", self.conn)
        
        #cleanup
        self.disconnectdb()
        
        return data  
    
    def gettableschema(self,table):
        #initiate
        self.connectdb() 
        qry="SELECT * FROM information_schema.columns WHERE table_name   = '{}';".format(table)

        data = pd.read_sql(qry, self.conn)
        
        #cleanup
        self.disconnectdb()
        
        return data

    def passqry(self,qry):
        #initiate
        self.connectdb()        
        
        data = pd.read_sql(qry, self.conn)
        
        #cleanup
        self.disconnectdb()
        
        return data
    
    def iplookup(self,ip):
        url = 'http://ipinfo.io/{}/json'.format(ip)
        response = urllib2.urlopen(url)
        data = json.load(response)
        
        return data
    
    def lookuprelays(self,limit):
        #get ips from cardano network
        df_relay=con.passqry("SELECT DISTINCT ipv4 from pool_relay WHERE ipv4 IS NOT NULL limit {}".format(limit))
        #apply lookup function
        df_relay["ipv4"]=df_relay["ipv4"].apply(con.iplookup)
        #convert json objects into new df
        df_relay=pd.json_normalize(df_relay["ipv4"])
        
        
        df_relay['org'].value_counts().plot(kind="bar")
        
        return df_relay
    
#%%
class analytics:
    def __init__(self):
        self.init=True
        
    def relayanalytics(numberofrelays,plot,numbertoplot):
        #%% get ip's for cardano relays and get IP metadata 
        df_relay=con.lookuprelays(numberofrelays)
        
        df_relay['Company']=df_relay['org'].str.split(expand=True)[1] + " " +df_relay['org'].str.split(expand=True)[2] + " " + df_relay['org'].str.split(expand=True)[3].fillna("")
        
        df_count=pd.DataFrame(df_relay['Company'].value_counts().copy())
        
        #calculates the estimated distribution
        df_count["Percentage"]=df_count["Company"]/sum(df_count["Company"])
        
        #estimates the total number of nodes based on the given samle
        df_count["Total"]=df_count["Percentage"]*32275

#%%
        if plot:
            df_head=df_count.head(numbertoplot)
            fig=px.bar(df_head,
                       x=df_head.index,
                       color_continuous_scale="geyser",
                       y="Total",
                       text="Total",
                       color='Total')
            
            fig.update_layout({
                    'plot_bgcolor':'rgba(0,0,0,0)',
                    'paper_bgcolor':'rgba(0,0,0,0)'
                    })
            
            fig.layout["xaxis"].title = ""
            
            fig.layout["yaxis"].title = ""
            fig.layout["yaxis"].tickfont = dict(color = 'rgba(0,0,0,0)')
            
            fig.update_xaxes(tickfont_size=14)
            
            imgurls={"DigitalOcean, LLC ": [r"https://iconape.com/wp-content/files/ht/53291/svg/digitalocean-icon-1.svg",0.050],
                         "Amazon.com, Inc. ": [r"https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg",0.125],
                         "Google LLC ": [r"https://upload.wikimedia.org/wikipedia/commons/thumb/2/2f/Google_2015_logo.svg/2560px-Google_2015_logo.svg.png",0.215]}
            start=.5
            current=start
            jump=1
            for i in imgurls.keys():
                amount=df_count.loc[i]["Total"]/max(df_count["Total"])-0.03
                src=imgurls[i][0]
                xplace=imgurls[i][1]
                fig.add_layout_image(dict(
                        source=str(src),
                        x=xplace,
                        y=amount,
                        )
                )
                    
                current+=jump
            
            fig.update_layout_images(dict(
                    xref="paper",
                    yref="paper",
                    sizex=0.08,
                    sizey=0.08,
                    xanchor="right",
                    yanchor="bottom"
            ))
            
            #write graph to folder
            path=os.path.join(os.getcwd(),"output","providers.html")
            fig.write_html(path)
            
        return df_relay,df_count
#%%
    def tracktransaction(tranactionid,layers,plot):
        
        starttxoutid=tranactionid
        temptxoutid=[starttxoutid]
        paths=pd.DataFrame()
        
        node_x=[]
        node_y=[]
        node_text=[]
        graph_text=[]
        tx_sizes=[]
        
        ls_edges=[]
                
        for i in range(layers):
            temptxoutid=[str(i) for i in temptxoutid]
            str_ls=", ".join(temptxoutid)
            edge=con.passqry("""SELECT tx_in_id,tx_out_id,tx.out_sum as tx_size, tx2.out_sum as tx_size_out from ((tx_in 
                             LEFT JOIN tx ON tx_in.tx_in_id=tx.id) 
                             LEFT JOIN tx tx2 ON tx_in.tx_out_id=tx2.id) 
                             WHERE tx_out_id IN ({})""".format(str_ls))
            
            mergecol="node_{}".format(i)
            newmergecol="node_{}".format(i+1)
            edge=edge.rename({"tx_in_id":newmergecol,"tx_out_id":mergecol},axis=1)
            
            temptxoutid=edge[newmergecol].tolist()
            
            tempdir=edge[[mergecol,"tx_size"]].to_dict("records")
            lookupdir={}
            for rec in tempdir:
                lookupdir[rec[mergecol]]=rec["tx_size"]
            
            ls_edges.extend(edge[[mergecol,newmergecol]].values.tolist())
                        
            #assignnodecordinates
            layer=0
            for j in edge[mergecol].unique().tolist():
                node_x.append(i)
                node_y.append(layer)
                tx_sizes.append(lookupdir[j])
                node_text.append(j)
                graph_text.append("tx " + str(j) +": "+ str(lookupdir[j]) + " ADA")
                layer+=1
            
            #center layer
            indices=[k for k, x in enumerate(node_x) if x == i]
            
            for l in indices:
                node_y[l]-=(len(indices)-1)/2
            
            #assign edges
            if paths.empty:
                paths=edge.copy()
            else:
                paths=paths.merge(edge,how="left",on=mergecol)
        
        tempdir=edge[[newmergecol,"tx_size_out"]].to_dict("records")
        lookupdir={}
        for rec in tempdir:
            lookupdir[rec[newmergecol]]=rec["tx_size_out"]
        
        
        #opretter sidste lag
        layer=0
        for j in edge[newmergecol].unique().tolist():
            node_x.append(i+1)
            node_y.append(layer)
            tx_sizes.append(lookupdir[j])
            node_text.append(j)
            graph_text.append("tx " + str(j) +": "+ str(lookupdir[j]) + " ADA")
            layer+=1
        
        #%%
        #center layer
        indices=[k for k, x in enumerate(node_x) if x == i+1]
        
        for l in indices:
            node_y[l]-=(len(indices)-1)/2
        
        #%%
        #opretter edges
        edge_x=[]
        edge_y=[]
        for i in ls_edges:
            x0=node_x[node_text.index(i[0])]
            y0=node_y[node_text.index(i[0])]
            
            x1=node_x[node_text.index(i[1])]
            y1=node_y[node_text.index(i[1])]
            
            edge_x.append(x0)
            edge_x.append(x1)
            edge_x.append(None)
            edge_y.append(y0)
            edge_y.append(y1)
            edge_y.append(None) 
        
        if plot:
            #Setting up node plot
            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers',
                hoverinfo='text',
                text=graph_text,
                marker=dict(
                    showscale=True,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color=tx_sizes,
                    size=10,
                    colorbar=dict(
                        thickness=15,
                        title='Transaction size',
                        xanchor='left',
                        titleside='right'
                    ),
                    line_width=2))
            #setting up edge plot
            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines')
            
            #setting up figure
            fig = go.Figure(data=[edge_trace, node_trace],
                         layout=go.Layout(
                            title='<br>Network graph for cardano transactions',
                            titlefont_size=16,
                            showlegend=False,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                            )
            
            #write graph to folder
            path=os.path.join(os.getcwd(),"output","network.html")
            fig.write_html(path)
        
        return node_x,node_y,edge_x,edge_y,tx_sizes


#%%
con=connect()