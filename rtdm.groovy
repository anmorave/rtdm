import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import com.sas.adapters.connectors.SASPoolDB as NewSASPoolDB;


import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator


public class GetOfertaNotificacionRecarga implements Runnable{

	String  grupo_prepago, tracking_esp,recarga_saldo ;
	Double monto_recarga;
		
	private String directo_menu, opcion_1, opcion_2, palabra_clave_1, palabra_clave_2, codigo_oferta1, codigo_oferta2, id_plantilla;
	
    private Boolean estado_groovy = true; 
	//recarga_saldo=0	
    private String query1 = "SELECT directo_menu, opcion_1, opcion_2, palabra_clave_1, palabra_clave_2, codigo_oferta1, codigo_oferta2, id_plantilla FROM DATAMART.RTDM_OFERTA_MENSAJES WHERE ?>=valor_recarga_inicial and ?<=valor_recarga_final and grupo_prepago=? and recarga_saldo=?";

	private static boolean LOG4J = false;
	def vars=null;
	private static final Logger LOGGER = Logger.getLogger("GetOfertaNotificacionRecarga");
	private static final SimpleDateFormat dateformat = new SimpleDateFormat ("yyyy.MM.dd hh:mm:ss a zzz");

	
	GetOfertaNotificacionRecarga(){
		/* Carga el archvo de properties para leer variables del aplicativo */
		Properties props = new Properties();
		File propsFile = new File('/saswork/sasprop/groovy.properties');
		props.load(propsFile.newDataInputStream());
		vars = new ConfigSlurper().parse(props);
		//level = Level.toLevel(vars.groovy.log4j.level);
		if (!LOG4J) {
			PropertyConfigurator.configure(vars.groovy.log4j.file);
			BasicConfigurator.configure();
			LOG4J = true;
		}
		
		
		
	}
	
	
    public void run() {
        PreparedStatement stmt1 = null;
        ResultSet rs1 = null;
        Connection con = null;
        
        try {
			if(LOGGER.isInfoEnabled())
			LOGGER.info("Start:  grupo_prepago: "+ grupo_prepago+ " tracking_esp: " + tracking_esp + " monto_recarga: "+ monto_recarga);
			            
            isEmpty(grupo_prepago,"grupo_prepago");
            isEmpty(tracking_esp,"tracking_esp");
			//isEmpty(monto_recarga,"monto_recarga");
		
			
            con = NewSASPoolDB.getConnection(); 

            stmt1 = con.prepareStatement(query1);

            stmt1.setDouble(1,monto_recarga);
	    stmt1.setDouble(2,monto_recarga);
	    stmt1.setString(3,grupo_prepago);
            stmt1.setString(4,recarga_saldo);
			
            rs1 = stmt1.executeQuery();			
			
           
		while (rs1.next()){
			directo_menu = rs1.getString("directo_menu");
			palabra_clave_1 = rs1.getString("palabra_clave_1");
			palabra_clave_2 = rs1.getString("palabra_clave_2");			
			opcion_1 = rs1.getString("opcion_1");			
			opcion_2 = rs1.getString("opcion_2");
			codigo_oferta1 = rs1.getString("codigo_oferta1");
			codigo_oferta2 = rs1.getString("codigo_oferta2");
			id_plantilla = rs1.getString("id_plantilla");
			
		 }
/*
		while (rs1.next()){
			directo_menu = rs1.getString("directo_menu").trim();
			palabra_clave_1 = rs1.getString("palabra_clave_1").trim();
			palabra_clave_2 = rs1.getString("palabra_clave_2").trim();			
			opcion_1 = rs1.getString("opcion_1").trim();			
			opcion_2 = rs1.getString("opcion_2").trim();
			codigo_oferta1 = rs1.getString("codigo_oferta1").trim();
			codigo_oferta2 = rs1.getString("codigo_oferta2").trim();
			id_plantilla = rs1.getString("id_plantilla").trim();
			
		 } */
	 
		 
		//isEmpty(tracking_esp_padre,"tracking_esp_padre");
		//isEmpty(monto_recarga_original,"monto_recarga_original");
		//isEmpty(monto_recarga_base,"monto_recarga_base");
		//isEmpty(monto_comision_pdv,"monto_comision_pdv");
		//isEmpty(boltoncode,"boltoncode");
		//isEmpty(boltontypecode,"boltontypecode");
		
	
            
        }catch(Exception ex){
			estado_groovy = false;
           LOGGER.error("Exception - tracking_esp: "+tracking_esp,ex);
            
        } finally {
            if (rs1 != null) {rs1.close();}
             if (stmt1 != null) {stmt1.close();}
            if (con != null) {con.close();}
        }
		
		if(LOGGER.isInfoEnabled())
		LOGGER.info("End: tracking_esp: "+tracking_esp +" - estado_groovy:" + estado_groovy );
    }

    
    private String getDate(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date);
    }
    
    private void isEmpty(String var, String varName) throws Exception{
        if(var == null){
            throw new Exception("Variable "+varName + " es Nula");
        }else if("".equals(var.trim())){
            throw new Exception("Variable "+varName + " es Vac\u00eda");
        }
    }
    
    	
	public String getDirecto_menu() {return directo_menu;}
	public String getPalabra_clave_1() {return palabra_clave_1;}
	public String getPalabra_clave_2() {return palabra_clave_2;}
	public String getOpcion_1() {return opcion_1;}
	public String getOpcion_2() {return opcion_2;}
	public String getCodigo_oferta1() {return codigo_oferta1;}
	public String getCodigo_oferta2() {return codigo_oferta2;}
	public String getId_plantilla() {return id_plantilla;}


    public Boolean getEstado_groovy() {return estado_groovy;}

    public static void main(String[] args) throws Exception {
		
        GetOfertaNotificacionRecarga var = new GetOfertaNotificacionRecarga();
        var.tracking_esp = "20190719_0854";
		var.monto_recarga = 2;
        var.grupo_prepago = "1";
        var.recarga_saldo = "0";


        var.run();
        
		 
		System.out.println("directo_menu= "+var.getDirecto_menu());
		System.out.println("opcion_1= "+var.getOpcion_1());
		System.out.println("opcion_2= "+var.getOpcion_2());
		System.out.println("palabra_clave_1= "+var.getPalabra_clave_1());
		System.out.println("palabra_clave_2= "+var.getPalabra_clave_2());
		System.out.println("codigo_oferta1= "+var.getCodigo_oferta1());
		System.out.println("codigo_oferta2= "+var.getCodigo_oferta2());		
		System.out.println("id_plantilla= "+var.getId_plantilla());		
		System.out.println("estado_groovy= "+var.getEstado_groovy());
    }
}