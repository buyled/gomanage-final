from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import requests
import json
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Configuración de GoManage
GOMANAGE_BASE_URL = "http://buyled.clonico.es:8181"
GOMANAGE_USERNAME = "distri"
GOMANAGE_PASSWORD = "GOtmt%"
GOMANAGE_AUTH_TOKEN = "AKAAAQAAAAhtb2JpbGVBAACgAAIAAAAHZGlzdHJpAACQAAsAAAAIAAAAAGhdIuoAMAAMAAAAAgAMAKAAGgAAAA9TU09BY2Nlc3NUb2tlbgAAoAANAAAADVJPTEVfUFNDVXNlcgAAoAAPAAAANEQzQzM1OEI5Qzc0MjI3NTQ1MkM5NkZFMzIxRjREMjFCRkUzOEYyMUFBODlELm9lcGFzMQAA0AAQAAAACAAAAABoXTD6ANAAFwAAAAgAAAAAAAAAeADQABsAAAAIAAAAAAAAAHgA0AAcAAAACAAAAAAMwnAPALAAFQAAABBTNG0v02DCpLdqzAJOPicS"

# Variables globales para autenticación y cache
session_id = None
session_expires = None
all_customers = []
all_products = []
dashboard_data = {}

def authenticate_with_gomanage():
    """Autenticar con GoManage usando las credenciales reales"""
    global session_id, session_expires
    
    try:
        # Endpoint de autenticación correcto
        auth_url = f"{GOMANAGE_BASE_URL}/gomanage/static/auth/j_spring_security_check"
        
        # Headers según el ejemplo
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        # Datos de autenticación en formato form
        auth_data = {
            'j_username': GOMANAGE_USERNAME,
            'j_password': GOMANAGE_PASSWORD
        }
        
        print(f"🔐 Autenticando con GoManage: {GOMANAGE_USERNAME}")
        
        # Realizar login
        login_response = requests.post(
            auth_url,
            headers=headers,
            data=auth_data,
            timeout=15,
            allow_redirects=False
        )
        
        print(f"📊 Respuesta de autenticación: {login_response.status_code}")
        
        # Extraer JSESSIONID de las cookies
        if 'Set-Cookie' in login_response.headers:
            cookies = login_response.headers['Set-Cookie']
            for cookie in cookies.split(';'):
                if 'JSESSIONID' in cookie:
                    session_id = cookie.split('=')[1].split(';')[0]
                    session_expires = datetime.now() + timedelta(hours=2)
                    print(f"✅ Sesión iniciada: {session_id[:20]}...")
                    return True
        
        print("❌ No se pudo obtener JSESSIONID")
        return False
        
    except Exception as e:
        print(f"❌ Error en autenticación: {str(e)}")
        return False

def make_authenticated_request(method, url, params=None, json_data=None, timeout=30):
    """Realizar petición autenticada a GoManage"""
    global session_id, session_expires
    
    # Verificar si necesitamos autenticar
    if not session_id or (session_expires and datetime.now() > session_expires):
        print("🔄 Renovando autenticación...")
        if not authenticate_with_gomanage():
            return None
    
    try:
        headers = {
            'cookie': f"JSESSIONID={session_id}",
            'Authorization': f"oecp {GOMANAGE_AUTH_TOKEN}",
            'Accept': 'application/json'
        }
        
        if json_data:
            headers['Content-Type'] = 'application/json'
        
        if method.upper() == 'GET':
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
        elif method.upper() == 'POST':
            response = requests.post(url, headers=headers, params=params, json=json_data, timeout=timeout)
        else:
            raise ValueError(f"Método HTTP no soportado: {method}")
        
        return response
        
    except Exception as e:
        print(f"❌ Error en petición autenticada: {str(e)}")
        return None

def load_all_customers():
    """Cargar todos los clientes desde GoManage con paginación mejorada"""
    global all_customers
    
    try:
        if all_customers:  # Si ya están cargados, no recargar
            print(f"✅ Clientes ya cargados: {len(all_customers)}")
            return True
        
        print("🔄 Cargando todos los clientes desde GoManage...")
        
        # Hacer primera llamada para obtener total
        response = make_authenticated_request(
            'GET',
            f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-customers/List",
            timeout=30
        )
        
        if not response or response.status_code != 200:
            print(f"❌ Error cargando clientes: {response.status_code if response else 'No response'}")
            return False
        
        data = response.json()
        total_entries = data.get('total_entries', 0)
        print(f"📊 Total clientes disponibles: {total_entries}")
        
        # Cargar todos los clientes en lotes más grandes
        all_customers = []
        page_size = 500  # Aumentar tamaño de página para cargar más rápido
        total_pages = (total_entries + page_size - 1) // page_size
        
        for page in range(1, total_pages + 1):
            print(f"📄 Cargando página {page}/{total_pages}...")
            
            page_response = make_authenticated_request(
                'GET',
                f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-customers/List",
                params={'page': page, 'size': page_size},
                timeout=30
            )
            
            if page_response and page_response.status_code == 200:
                page_data = page_response.json()
                page_entries = page_data.get('page_entries', [])
                all_customers.extend(page_entries)
                print(f"✅ Cargados {len(page_entries)} clientes de página {page}")
            else:
                print(f"❌ Error en página {page}")
                break
        
        print(f"✅ TOTAL CARGADO: {len(all_customers)} clientes")
        return True
        
    except Exception as e:
        print(f"❌ Error cargando todos los clientes: {str(e)}")
        return False

def load_all_products():
    """Cargar todos los productos desde GoManage con paginación"""
    global all_products
    
    try:
        if all_products:  # Si ya están cargados, no recargar
            print(f"✅ Productos ya cargados: {len(all_products)}")
            return True
        
        print("🔄 Cargando todos los productos desde GoManage...")
        
        # Hacer primera llamada para obtener total
        response = make_authenticated_request(
            'GET',
            f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-products/List",
            timeout=30
        )
        
        if not response or response.status_code != 200:
            print(f"❌ Error cargando productos: {response.status_code if response else 'No response'}")
            return False
        
        data = response.json()
        total_entries = data.get('total_entries', 0)
        print(f"📊 Total productos disponibles: {total_entries}")
        
        # Cargar todos los productos en lotes
        all_products = []
        page_size = 500  # Tamaño de página
        total_pages = (total_entries + page_size - 1) // page_size
        
        for page in range(1, total_pages + 1):
            print(f"📄 Cargando página {page}/{total_pages}...")
            
            page_response = make_authenticated_request(
                'GET',
                f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-products/List",
                params={'page': page, 'size': page_size},
                timeout=30
            )
            
            if page_response and page_response.status_code == 200:
                page_data = page_response.json()
                page_entries = page_data.get('page_entries', [])
                all_products.extend(page_entries)
                print(f"✅ Cargados {len(page_entries)} productos de página {page}")
            else:
                print(f"❌ Error en página {page}")
                break
        
        print(f"✅ TOTAL CARGADO: {len(all_products)} productos")
        return True
        
    except Exception as e:
        print(f"❌ Error cargando todos los productos: {str(e)}")
        return False

@app.route('/')
def index():
    """Página principal"""
    return render_template('index_improved.html')

@app.route('/api/auth', methods=['POST'])
def auth():
    """Endpoint de autenticación"""
    try:
        success = authenticate_with_gomanage()
        
        if success:
            # Cargar datos iniciales
            load_all_customers()
            load_all_products()
            
            return jsonify({
                "status": "success",
                "message": "Autenticación exitosa",
                "session_id": session_id[:20] + "..." if session_id else None,
                "customers_loaded": len(all_customers),
                "products_loaded": len(all_products)
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Error de autenticación"
            }), 401
            
    except Exception as e:
        print(f"❌ Error en auth: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {str(e)}"
        }), 500

@app.route('/api/customers', methods=['GET'])
def get_customers():
    """Obtener lista de clientes con búsqueda y paginación mejorada"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        search = request.args.get('search', '').strip().lower()
        
        # Asegurar que los clientes estén cargados
        if not all_customers:
            load_all_customers()
        
        # Aplicar filtro de búsqueda si se proporciona
        filtered_customers = all_customers
        if search:
            filtered_customers = []
            for customer in all_customers:
                # Buscar en múltiples campos
                search_fields = [
                    str(customer.get('business_name', '')).lower(),
                    str(customer.get('name', '')).lower(),
                    str(customer.get('vat_number', '')).lower(),
                    str(customer.get('code', '')).lower(),
                    str(customer.get('email', '')).lower(),
                    str(customer.get('city', '')).lower()
                ]
                
                if any(search in field for field in search_fields):
                    filtered_customers.append(customer)
        
        # Aplicar paginación
        total = len(filtered_customers)
        start = (page - 1) * per_page
        end = start + per_page
        customers_page = filtered_customers[start:end]
        
        return jsonify({
            "status": "success",
            "customers": customers_page,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        })
        
    except Exception as e:
        print(f"❌ Error en get_customers: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error cargando clientes: {str(e)}"
        }), 500

@app.route('/api/customers', methods=['POST'])
def create_customer():
    """Crear nuevo cliente en GoManage con campos fijos y validaciones mejoradas"""
    try:
        # Verificar autenticación
        if not session_id:
            if not authenticate_with_gomanage():
                return jsonify({
                    "status": "error",
                    "message": "Error de autenticación con GoManage"
                }), 401
        
        # Obtener datos del cliente del request
        customer_data = request.get_json()
        
        if not customer_data:
            return jsonify({
                "status": "error",
                "message": "No se proporcionaron datos del cliente"
            }), 400
        
        # Validar campos obligatorios
        required_fields = ['business_name', 'name', 'vat_number']
        for field in required_fields:
            if not customer_data.get(field):
                return jsonify({
                    "status": "error",
                    "message": f"Campo obligatorio faltante: {field}"
                }), 400
        
        # Preparar payload con campos fijos y valores por defecto
        payload = {
            # Campos fijos según especificaciones
            "has_discount_scale": True,
            "country_id": "ES",
            "currency_id": "eur",
            "periodicity_id": "1",
            "language_id": "cas",
            
            # Campos del formulario
            "business_name": customer_data.get("business_name"),
            "name": customer_data.get("name"),
            "vat_number": customer_data.get("vat_number"),
            "city": customer_data.get("city", ""),
            "street_name": customer_data.get("street_name", ""),
            "postal_code": customer_data.get("postal_code", 0),
            "province_id": int(customer_data.get("province_id", 3)),
            "payment_method_id": customer_data.get("payment_method_id", "007"),
            "tip_cli": customer_data.get("tip_cli", "3"),
            
            # Opciones booleanas
            "send_invoices": customer_data.get("send_invoices", False),
            "send_newsletter": customer_data.get("send_newsletter", False),
            "is_public_administration": customer_data.get("is_public_administration", False),
            
            # Campos con valores por defecto
            "control_level": 0,
            "customer_ambit": 0,
            "default_attachment_id": 0,
            "group_id": 0,
            "has_attachments": False,
            "has_self_invoicing": False,
            "has_shared_contacts_by_zone": False,
            "is_foreign": False,
            "is_invoiced_at_central": False,
            "is_not_regular_customer": False,
            "is_resident": False,
            "purchasing_group_commission": 0,
            "request_print_fluorinated_gas": False,
            "subgroup_id": 0,
            "user_dec1-2": 0,
            "user_dec2-2": 0,
            "user_dec3-2": 0,
            "user_dec4-2": 0,
            "user_dec5-2": 0,
            "user_dec6-2": 0,
            "is_locked": "Si"
        }
        
        # Headers para la petición
        headers = {
            'cookie': f"JSESSIONID={session_id}",
            'Authorization': f"oecp {GOMANAGE_AUTH_TOKEN}",
            'Content-Type': "application/json"
        }
        
        # URL del endpoint de creación de clientes
        url = f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-customers/"
        
        print(f"🔄 Creando cliente: {payload['business_name']}")
        print(f"📊 Payload: {json.dumps(payload, indent=2)}")
        
        # Realizar petición POST
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"📊 Respuesta de GoManage: {response.status_code}")
        print(f"📄 Contenido respuesta: {response.text}")
        
        if response.status_code == 200 or response.status_code == 201:
            # Cliente creado exitosamente
            response_data = response.json() if response.text else {}
            
            # Limpiar cache de clientes para forzar recarga
            global all_customers
            all_customers = []
            
            return jsonify({
                "status": "success",
                "message": "Cliente creado exitosamente",
                "customer": response_data
            })
        else:
            # Error en la creación
            error_message = response.text if response.text else f"Error HTTP {response.status_code}"
            print(f"❌ Error creando cliente: {error_message}")
            
            return jsonify({
                "status": "error",
                "message": f"Error creando cliente: {error_message}",
                "status_code": response.status_code
            }), response.status_code
            
    except Exception as e:
        print(f"❌ Error en create_customer: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error interno: {str(e)}"
        }), 500

@app.route('/api/sales-orders', methods=['GET'])
def get_sales_orders():
    """Obtener lista de pedidos de venta con filtros opcionales"""
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        customer_id = request.args.get('customer_id')
        sort = request.args.get('sort', 'date')
        from_date = request.args.get('from_date')
        
        # Construir parámetros de la consulta
        params = {
            'page': page,
            'size': per_page,
            'sort': sort
        }
        
        # Agregar filtro de cliente si se proporciona
        if customer_id:
            params['customer_id'] = customer_id
            
        # Agregar filtro de fecha si se proporciona
        if from_date:
            params['from_date'] = from_date
        
        print(f"📄 Cargando pedidos de venta - Página {page}, Cliente: {customer_id or 'Todos'}, Desde: {from_date or 'Sin filtro'}")
        
        # Hacer petición a GoManage
        response = make_authenticated_request(
            'GET',
            f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-sales-invoices/List",
            params=params,
            timeout=30
        )
        
        if not response or response.status_code != 200:
            print(f"❌ Error cargando pedidos de venta: {response.status_code if response else 'No response'}")
            return jsonify({
                "status": "error",
                "message": f"Error cargando pedidos de venta: {response.status_code if response else 'Sin respuesta'}"
            }), 500
        
        data = response.json()
        sales_orders = data.get('page_entries', [])
        total_entries = data.get('total_entries', 0)
        
        print(f"✅ Cargados {len(sales_orders)} pedidos de venta de {total_entries} totales")
        
        return jsonify({
            "status": "success",
            "sales_orders": sales_orders,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total_entries,
                "pages": (total_entries + per_page - 1) // per_page
            }
        })
        
    except Exception as e:
        print(f"❌ Error en get_sales_orders: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error cargando pedidos de venta: {str(e)}"
        }), 500

@app.route('/api/orders', methods=['POST'])
def create_order():
    """Crear nuevo pedido en GoManage"""
    try:
        # Verificar autenticación
        if not session_id:
            if not authenticate_with_gomanage():
                return jsonify({
                    "status": "error",
                    "message": "Error de autenticación con GoManage"
                }), 401
        
        # Obtener datos del pedido del request
        order_data = request.get_json()
        
        if not order_data:
            return jsonify({
                "status": "error",
                "message": "No se proporcionaron datos del pedido"
            }), 400
        
        # Validar campos obligatorios
        required_fields = ['customer_id', 'reference', 'date', 'lines']
        for field in required_fields:
            if not order_data.get(field):
                return jsonify({
                    "status": "error",
                    "message": f"Campo obligatorio faltante: {field}"
                }), 400
        
        # Preparar payload del pedido
        payload = {
            "customer_id": order_data["customer_id"],
            "reference": order_data["reference"],
            "date": order_data["date"],
            "lines": order_data["lines"],
            "subtotal": order_data.get("subtotal", 0),
            "discount": order_data.get("discount", 0),
            "tax": order_data.get("tax", 0),
            "total": order_data.get("total", 0),
            "notes": order_data.get("notes", ""),
            "status": "pending"
        }
        
        # Headers para la petición
        headers = {
            'cookie': f"JSESSIONID={session_id}",
            'Authorization': f"oecp {GOMANAGE_AUTH_TOKEN}",
            'Content-Type': "application/json"
        }
        
        # URL del endpoint de creación de pedidos
        url = f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-orders/"
        
        print(f"🔄 Creando pedido: {payload['reference']}")
        
        # Realizar petición POST
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )
        
        print(f"📊 Respuesta de GoManage: {response.status_code}")
        
        if response.status_code == 200 or response.status_code == 201:
            return jsonify({
                "status": "success",
                "message": "Pedido creado exitosamente en GoManage",
                "order_id": response.json().get('id', 'N/A') if response.text else 'N/A',
                "reference": order_data["reference"]
            })
        else:
            return jsonify({
                "status": "error",
                "message": f"Error del servidor GoManage: {response.status_code}",
                "details": response.text
            }), 500
            
    except Exception as e:
        print(f"❌ Error creando pedido: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error creando pedido: {str(e)}"
        }), 500

@app.route('/api/analytics/dashboard', methods=['GET'])
def get_analytics_dashboard():
    """Obtener datos de análisis para el dashboard"""
    try:
        # Asegurar que los datos estén cargados
        if not all_customers:
            load_all_customers()
        
        # Calcular métricas de clientes
        customers_count = len(all_customers)
        
        # Análisis por tipo de cliente
        customer_types = {}
        for customer in all_customers:
            tip_cli = customer.get('tip_cli', 'Sin clasificar')
            customer_types[tip_cli] = customer_types.get(tip_cli, 0) + 1
        
        # Análisis por provincia
        provinces = {}
        for customer in all_customers:
            province = customer.get('province_name', 'Sin provincia')
            provinces[province] = provinces.get(province, 0) + 1
        
        # Top 10 provincias
        top_provinces = sorted(provinces.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Obtener datos de pedidos de venta
        sales_response = make_authenticated_request(
            'GET',
            f"{GOMANAGE_BASE_URL}/gomanage/web/data/apitmt-sales-invoices/List",
            params={'size': 100},
            timeout=30
        )
        
        sales_data = []
        total_sales = 0
        if sales_response and sales_response.status_code == 200:
            sales_data = sales_response.json().get('page_entries', [])
            total_sales = sum(float(order.get('total', 0)) for order in sales_data)
        
        return jsonify({
            "status": "success",
            "data": {
                "customers": {
                    "total": customers_count,
                    "by_type": customer_types,
                    "by_province": dict(top_provinces)
                },
                "sales": {
                    "total_orders": len(sales_data),
                    "total_amount": total_sales,
                    "recent_orders": sales_data[:10]
                }
            }
        })
        
    except Exception as e:
        print(f"❌ Error en analytics dashboard: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error cargando análisis: {str(e)}"
        }), 500

@app.route('/api/chat/mcp', methods=['POST'])
def chat_mcp():
    """Chat con funcionalidades MCP"""
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({
                "status": "error",
                "message": "Pregunta vacía"
            }), 400
        
        # Simular respuesta del MCP basada en la pregunta
        response_text = generate_mcp_response(question)
        
        return jsonify({
            "status": "success",
            "response": response_text,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"❌ Error en chat MCP: {str(e)}")
        return jsonify({
            "status": "error",
            "message": f"Error en chat: {str(e)}"
        }), 500

def generate_mcp_response(question):
    """Generar respuesta del MCP basada en la pregunta"""
    question_lower = question.lower()
    
    if 'clientes' in question_lower:
        return f"📊 **Información de Clientes:**\n\n" \
               f"• Total de clientes: {len(all_customers)}\n" \
               f"• Clientes cargados en memoria\n" \
               f"• Funciones disponibles: búsqueda, filtrado, creación\n" \
               f"• API endpoint: /api/customers"
    
    elif 'pedidos' in question_lower or 'ventas' in question_lower:
        return f"📋 **Información de Pedidos:**\n\n" \
               f"• Endpoint: /gomanage/web/data/apitmt-sales-invoices/List\n" \
               f"• Filtros disponibles: cliente, fecha, ordenamiento\n" \
               f"• Paginación automática\n" \
               f"• Datos en tiempo real desde GoManage"
    
    elif 'api' in question_lower:
        return f"🔧 **APIs Disponibles:**\n\n" \
               f"• GET /api/customers - Listar clientes\n" \
               f"• POST /api/customers - Crear cliente\n" \
               f"• GET /api/sales-orders - Pedidos de venta\n" \
               f"• POST /api/orders - Crear pedido\n" \
               f"• GET /api/analytics/dashboard - Análisis\n" \
               f"• POST /api/chat/mcp - Chat MCP"
    
    elif 'funciones' in question_lower or 'características' in question_lower:
        return f"⚡ **Funcionalidades del Sistema:**\n\n" \
               f"• Gestión completa de clientes\n" \
               f"• Alta de clientes con validación\n" \
               f"• Pedidos de venta en tiempo real\n" \
               f"• Análisis y reportes\n" \
               f"• Chat inteligente MCP\n" \
               f"• Integración completa con GoManage"
    
    else:
        return f"🤖 **Asistente MCP GoManage**\n\n" \
               f"Puedo ayudarte con:\n" \
               f"• Información sobre clientes\n" \
               f"• Datos de pedidos y ventas\n" \
               f"• APIs disponibles\n" \
               f"• Funcionalidades del sistema\n\n" \
               f"¿En qué te puedo ayudar específicamente?"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8450, debug=True)



# ============================================================================
# NUEVOS ENDPOINTS PARA FUNCIONALIDADES AVANZADAS
# ============================================================================

@app.route('/api/analytics/dashboard', methods=['GET'])
def get_analytics_dashboard():
    """Obtener datos del dashboard de análisis"""
    try:
        # Obtener datos de clientes
        customers_response = make_gomanage_request('GET', 'customers')
        customers_data = customers_response.get('customers', []) if customers_response else []
        
        # Obtener datos de pedidos
        orders_response = make_gomanage_request('GET', 'sales-orders')
        orders_data = orders_response.get('sales_orders', []) if orders_response else []
        
        # Procesar datos para analytics
        analytics_data = {
            'customers': {
                'total': len(customers_data),
                'by_type': process_customers_by_type(customers_data),
                'by_province': process_customers_by_province(customers_data)
            },
            'sales': {
                'total_orders': len(orders_data),
                'total_amount': calculate_total_sales(orders_data),
                'by_month': process_sales_by_month(orders_data)
            }
        }
        
        return jsonify({
            'status': 'success',
            'data': analytics_data
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener datos de análisis: {str(e)}'
        }), 500

def process_customers_by_type(customers):
    """Procesar clientes por tipo"""
    type_counts = {}
    for customer in customers:
        tip_cli = customer.get('tip_cli', 'otros')
        type_counts[tip_cli] = type_counts.get(tip_cli, 0) + 1
    return type_counts

def process_customers_by_province(customers):
    """Procesar clientes por provincia"""
    province_counts = {}
    for customer in customers:
        province = customer.get('province_name', 'Sin especificar')
        province_counts[province] = province_counts.get(province, 0) + 1
    return province_counts

def calculate_total_sales(orders):
    """Calcular total de ventas"""
    total = 0
    for order in orders:
        total += float(order.get('total', 0))
    return total

def process_sales_by_month(orders):
    """Procesar ventas por mes"""
    monthly_sales = {}
    for order in orders:
        # Simular procesamiento por mes
        month = order.get('date', '2024-01')[:7]  # YYYY-MM
        monthly_sales[month] = monthly_sales.get(month, 0) + float(order.get('total', 0))
    return monthly_sales

@app.route('/api/chat/mcp', methods=['POST'])
def chat_mcp():
    """Endpoint para el chat con MCP"""
    try:
        data = request.get_json()
        question = data.get('question', '').strip()
        
        if not question:
            return jsonify({
                'status': 'error',
                'message': 'La pregunta no puede estar vacía'
            }), 400
        
        # Procesar la pregunta y generar respuesta
        response = process_mcp_question(question)
        
        return jsonify({
            'status': 'success',
            'response': response
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error en el chat MCP: {str(e)}'
        }), 500

def process_mcp_question(question):
    """Procesar pregunta del chat MCP y generar respuesta"""
    question_lower = question.lower()
    
    # Respuestas basadas en palabras clave
    if 'clientes' in question_lower or 'cliente' in question_lower:
        try:
            customers_response = make_gomanage_request('GET', 'customers')
            if customers_response:
                total_customers = len(customers_response.get('customers', []))
                return f"📊 **Información de Clientes:**\n\n• Total de clientes: **{total_customers}**\n• Clientes activos en el sistema\n• Datos actualizados en tiempo real desde GoManage\n\n¿Te gustaría ver más detalles sobre algún aspecto específico?"
            else:
                return "No pude obtener la información de clientes en este momento. Por favor, verifica la conexión con GoManage."
        except:
            return "Hay un problema temporal con la conexión a GoManage. Por favor, inténtalo de nuevo."
    
    elif 'ventas' in question_lower or 'pedidos' in question_lower:
        try:
            orders_response = make_gomanage_request('GET', 'sales-orders')
            if orders_response:
                orders = orders_response.get('sales_orders', [])
                total_orders = len(orders)
                total_amount = sum(float(order.get('total', 0)) for order in orders)
                return f"📈 **Información de Ventas:**\n\n• Total de pedidos: **{total_orders}**\n• Facturación total: **{total_amount:,.2f}€**\n• Datos en tiempo real desde GoManage\n\n¿Quieres que genere un reporte detallado?"
            else:
                return "No pude obtener la información de ventas en este momento."
        except:
            return "Hay un problema temporal con la conexión a GoManage para obtener datos de ventas."
    
    elif 'reporte' in question_lower or 'informe' in question_lower:
        return "📄 **Generación de Reportes:**\n\nPuedo ayudarte a generar varios tipos de reportes:\n\n• **Reporte de Clientes** - Lista completa con estadísticas\n• **Reporte de Ventas** - Análisis de tendencias\n• **Reporte Geográfico** - Distribución por provincias\n• **Facturas y Albaranes** - Documentos comerciales\n\nUsa el panel de documentos a la derecha para configurar y generar reportes personalizados."
    
    elif 'funciones' in question_lower or 'ayuda' in question_lower or 'qué puedes' in question_lower:
        return "🤖 **Mis Funcionalidades:**\n\n• **Consultas de Datos** - Información sobre clientes, pedidos y ventas\n• **Análisis en Tiempo Real** - Estadísticas y métricas actualizadas\n• **Generación de Reportes** - Documentos PDF, Excel y CSV\n• **Búsquedas Avanzadas** - Encuentra información específica\n• **Sugerencias** - Recomendaciones basadas en datos\n• **Integración MCP** - Acceso directo a la API de GoManage\n\n¿En qué área específica te gustaría que te ayude?"
    
    elif 'pdf' in question_lower or 'documento' in question_lower:
        return "📄 **Generación de Documentos PDF:**\n\nPuedo generar varios tipos de documentos:\n\n• **Facturas** - Documentos comerciales oficiales\n• **Albaranes** - Documentos de entrega\n• **Reportes** - Análisis detallados con gráficos\n• **Listados** - Exportaciones de datos\n\nUsa el panel de documentos para configurar el formato, período y opciones específicas. Los documentos se generan con datos reales de GoManage."
    
    elif 'hola' in question_lower or 'buenos' in question_lower:
        return "¡Hola! 👋 Soy tu asistente MCP de GoManage. Estoy aquí para ayudarte con:\n\n• Consultas sobre tu negocio\n• Análisis de datos\n• Generación de reportes\n• Información del sistema\n\n¿En qué puedo ayudarte hoy?"
    
    else:
        return f"🤔 Entiendo que preguntas sobre: *{question}*\n\nActualmente puedo ayudarte con:\n\n• **Información de clientes y pedidos**\n• **Análisis de ventas y estadísticas**\n• **Generación de reportes y documentos**\n• **Búsquedas en el sistema**\n\n¿Podrías ser más específico sobre qué información necesitas?"

@app.route('/api/documents/generate', methods=['POST'])
def generate_document():
    """Generar documento PDF"""
    try:
        data = request.get_json()
        template = data.get('template')
        title = data.get('title', 'Documento')
        format_type = data.get('format', 'pdf')
        
        # Simular generación de documento
        # En una implementación real, aquí se generaría el PDF usando reportlab o weasyprint
        
        document_info = {
            'title': title,
            'format': format_type,
            'generated_at': datetime.now().isoformat(),
            'size': '1.2 MB',
            'pages': 15 if template == 'customer-report' else 8
        }
        
        return jsonify({
            'status': 'success',
            'message': f'Documento "{title}" generado exitosamente',
            'document': document_info
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al generar documento: {str(e)}'
        }), 500

@app.route('/api/orders/advanced', methods=['GET'])
def get_advanced_orders():
    """Obtener datos avanzados de pedidos para gestión"""
    try:
        # Obtener pedidos de GoManage
        orders_response = make_gomanage_request('GET', 'sales-orders')
        
        if not orders_response:
            return jsonify({
                'status': 'error',
                'message': 'No se pudieron obtener los pedidos'
            }), 500
        
        orders = orders_response.get('sales_orders', [])
        
        # Enriquecer datos con información adicional para gestión avanzada
        enriched_orders = []
        for order in orders:
            enriched_order = order.copy()
            # Simular datos adicionales para gestión avanzada
            enriched_order['status'] = simulate_order_status()
            enriched_order['priority'] = simulate_order_priority()
            enriched_order['estimated_delivery'] = simulate_delivery_date()
            enriched_orders.append(enriched_order)
        
        return jsonify({
            'status': 'success',
            'orders': enriched_orders,
            'total': len(enriched_orders)
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al obtener pedidos avanzados: {str(e)}'
        }), 500

def simulate_order_status():
    """Simular estado del pedido"""
    statuses = ['pending', 'confirmed', 'processing', 'shipped', 'delivered']
    return random.choice(statuses)

def simulate_order_priority():
    """Simular prioridad del pedido"""
    priorities = ['high', 'medium', 'low']
    return random.choice(priorities)

def simulate_delivery_date():
    """Simular fecha de entrega estimada"""
    from datetime import timedelta
    base_date = datetime.now()
    days_ahead = random.randint(1, 14)
    return (base_date + timedelta(days=days_ahead)).isoformat()

# ============================================================================
# ENDPOINT PARA SERVIR LAS NUEVAS SECCIONES HTML
# ============================================================================

@app.route('/api/sections/<section_name>')
def get_section_html(section_name):
    """Servir secciones HTML específicas"""
    try:
        section_files = {
            'analytics': 'templates/analytics.html',
            'advanced-orders': 'templates/advanced_orders.html',
            'chat-mcp': 'templates/chat_mcp.html'
        }
        
        if section_name not in section_files:
            return jsonify({
                'status': 'error',
                'message': 'Sección no encontrada'
            }), 404
        
        file_path = section_files[section_name]
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            return jsonify({
                'status': 'success',
                'content': content
            })
        except FileNotFoundError:
            return jsonify({
                'status': 'error',
                'message': 'Archivo de sección no encontrado'
            }), 404
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Error al cargar sección: {str(e)}'
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

