import mysql.connector
from mysql.connector import Error
from typing import List, Dict, Optional, Any, Tuple

class DatabaseConnector:
    def __init__(self, host: str = 'localhost', user: str = 'root', password: str = '', database: str = 'python'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None

    def connect(self) -> bool:
        """Establish connection to MySQL database"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.connection.is_connected():
                self.cursor = self.connection.cursor(dictionary=True)
                return True
        except Error as e:
            print(f"Error connecting to MySQL database: {e}")
            return False

    def close(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """Execute a SELECT query and return results"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            self.cursor.execute(query, params or ())
            result = self.cursor.fetchall()
            return result
        except Error as e:
            print(f"Error executing query: {e}")
            return None

    def execute_insert(self, query: str, params: Optional[Tuple] = None) -> bool:
        """Execute an INSERT query and commit changes"""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()
            
            self.cursor.execute(query, params or ())
            self.connection.commit()
            return True
        except Error as e:
            print(f"Error executing insert: {e}")
            return False

    def get_client_info(self, cliente: str, cadena: str = 'cruz verde') -> Optional[Dict[str, Any]]:
        """Get client information from database"""
        query = """
            SELECT c.*, cl.Nombre, cu.archivo_venta, cu.archivo_inventario 
            FROM cliente c
            JOIN clientes cl ON c.id_cliente = cl.id_cliente
            LEFT JOIN cliente_unidad_negocio cu ON c.id = cu.cliente_id
            WHERE c.cliente = %s AND c.cadena = %s
        """
        result = self.execute_query(query, (cliente, cadena))
        return result[0] if result else None

    def check_report_status(self, date: str, cliente: str, cadena: str = 'cruz verde') -> bool:
        """Check if report was already generated for given date"""
        query = """
            SELECT estado FROM log_script_descarga_cadena_cliente 
            WHERE cliente = %s AND cadena = %s AND DATE(created_at) = %s
        """
        result = self.execute_query(query, (cliente, cadena, date))
        return bool(result and result[0]['estado'] == 1)

    def log_report_generation(self, cliente: str, cadena: str, status: int = 0) -> bool:
        """Log report generation attempt"""
        query = """
            INSERT INTO log_script_descarga_cadena_cliente (cliente, cadena, created_at, estado)
            VALUES (%s, %s, NOW(), %s)
        """
        return self.execute_insert(query, (cliente, cadena, status))

    def update_report_status(self, cliente: str, cadena: str, status: int = 1) -> bool:
        """Update report generation status"""
        query = """
            UPDATE log_script_descarga_cadena_cliente 
            SET estado = %s, updated_at = NOW()
            WHERE cliente = %s AND cadena = %s 
            ORDER BY id DESC LIMIT 1
        """
        return self.execute_insert(query, (status, cliente, cadena))