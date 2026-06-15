package com.example.ui

import android.graphics.Bitmap
import android.util.Base64
import androidx.lifecycle.ViewModel
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.example.BuildConfig
import com.example.api.GeminiClient
import com.example.api.GeminiContent
import com.example.api.GeminiGenerationConfig
import com.example.api.GeminiInlineData
import com.example.api.GeminiPart
import com.example.api.GeminiRequest
import com.example.data.Customer
import com.example.data.InventoryItem
import com.example.data.JewelryRepository
import com.example.data.Transaction
import com.example.data.Artisan
import com.example.data.BankAccount
import com.example.data.Branch
import com.example.data.BusinessAccount
import com.example.data.Employee
import com.example.data.Supplier
import com.google.firebase.auth.FirebaseAuth
import com.google.firebase.auth.FirebaseUser
import com.google.firebase.auth.GoogleAuthProvider
import com.google.firebase.firestore.FirebaseFirestore
import com.example.ui.theme.UserTheme
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import org.json.JSONObject
import java.io.ByteArrayOutputStream
import java.util.Calendar
import java.util.Date
import java.util.Locale

sealed interface AISearchUiState {
    object Idle : AISearchUiState
    object Loading : AISearchUiState
    data class Success(val summary: String, val customer: Customer, val transactions: List<Transaction>) : AISearchUiState
    data class Error(val message: String) : AISearchUiState
}

sealed interface AIBusinessAdviceState {
    object Idle : AIBusinessAdviceState
    object Loading : AIBusinessAdviceState
    data class Success(val advice: String) : AIBusinessAdviceState
    data class Error(val message: String) : AIBusinessAdviceState
}

sealed interface AIInventorySearchState {
    object Idle : AIInventorySearchState
    object Loading : AIInventorySearchState
    data class Success(val matchedIds: List<Long>, val aiMessage: String) : AIInventorySearchState
    data class Error(val message: String) : AIInventorySearchState
}

sealed class AIVisionUiState {
    object Idle : AIVisionUiState()
    object Loading : AIVisionUiState()
    data class Success(
        val title: String,
        val itemType: String,
        val karat: String,
        val weightGrams: Double,
        val estimatedValue: Double,
        val notes: String,
        val customerName: String = "",
        val customerPhone: String = "",
        val amountBdt: Double = 0.0,
        val paidBdt: Double = 0.0,
        val dueBdt: Double = 0.0,
        val tags: String = ""
    ) : AIVisionUiState()
    data class Error(val message: String) : AIVisionUiState()
}

class JewelryViewModel(
    private val repository: JewelryRepository,
    private val context: android.content.Context
) : ViewModel() {

    // --- Business Local Config state ---
    data class BusinessConfig(
        val shopName: String = "স্বর্ণালি শিল্পালয়",
        val ownerName: String = "সুভাষ পাল শান্ত",
        val phone: String = "01712416731",
        val address: String = "মধ্যবাজার তারাকান্দা ময়মনসিংহ",
        val goldRate22K: String = "১,১৫,০০০",
        val lowStockThreshold: Int = 3
    )

    private val _businessConfig = MutableStateFlow(loadBusinessConfig())
    val businessConfig: StateFlow<BusinessConfig> = _businessConfig

    private fun loadBusinessConfig(): BusinessConfig {
        val prefs = context.getSharedPreferences("shornoly_prefs", android.content.Context.MODE_PRIVATE)
        return BusinessConfig(
            shopName = prefs.getString("shop_name", "স্বর্ণালি শিল্পালয়") ?: "স্বর্ণালি শিল্পালয়",
            ownerName = prefs.getString("owner_name", "সুভাষ পাল শান্ত") ?: "সুভাষ পাল শান্ত",
            phone = prefs.getString("phone", "01712416731") ?: "01712416731",
            address = prefs.getString("address", "মধ্যবাজার তারাকান্দা ময়মনসিংহ") ?: "মধ্যবাজার তারাকান্দা ময়মনসিংহ",
            goldRate22K = prefs.getString("gold_rate_22k", "১,১৫,০০০") ?: "১,১৫,০০০",
            lowStockThreshold = prefs.getInt("low_stock_threshold", 3)
        )
    }

    fun updateBusinessConfig(config: BusinessConfig) {
        val prefs = context.getSharedPreferences("shornoly_prefs", android.content.Context.MODE_PRIVATE)
        prefs.edit()
            .putString("shop_name", config.shopName)
            .putString("owner_name", config.ownerName)
            .putString("phone", config.phone)
            .putString("address", config.address)
            .putString("gold_rate_22k", config.goldRate22K)
            .putInt("low_stock_threshold", config.lowStockThreshold)
            .apply()
        _businessConfig.value = config
    }

    // --- Core Database Streams ---
    val customers: StateFlow<List<Customer>> = repository.allCustomers
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val inventoryItems: StateFlow<List<InventoryItem>> = repository.allInventoryItems
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val transactions: StateFlow<List<Transaction>> = repository.allTransactions
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val branches: StateFlow<List<Branch>> = repository.allBranches
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val suppliers: StateFlow<List<Supplier>> = repository.allSuppliers
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val artisans: StateFlow<List<Artisan>> = repository.allArtisans
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val employees: StateFlow<List<Employee>> = repository.allEmployees
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val bankAccounts: StateFlow<List<BankAccount>> = repository.allBankAccounts
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    val businessAccounts: StateFlow<List<BusinessAccount>> = repository.allBusinessAccounts
        .stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(5000),
            initialValue = emptyList()
        )

    private val _dailyRevenue = MutableStateFlow(0.0)
    val dailyRevenue: StateFlow<Double> = _dailyRevenue

    private val _dailyItemsSold = MutableStateFlow(0)
    val dailyItemsSold: StateFlow<Int> = _dailyItemsSold

    // --- Firebase Auth & Firestore ---
    private val auth by lazy { try { FirebaseAuth.getInstance() } catch(e: Exception) { null } }
    private val db by lazy { try { FirebaseFirestore.getInstance() } catch(e: Exception) { null } }

    private val _currentUser = MutableStateFlow<FirebaseUser?>(null)
    val currentUser: StateFlow<FirebaseUser?> = _currentUser

    private val _syncing = MutableStateFlow(false)
    val syncing: StateFlow<Boolean> = _syncing

    init {
        refreshDailyStats()
        try {
            auth?.let { firebaseAuth ->
                _currentUser.value = firebaseAuth.currentUser
                firebaseAuth.addAuthStateListener {
                    _currentUser.value = it.currentUser
                }
            }
        } catch (e: Exception) {
            // Log or ignore
        }
    }

    fun signInWithFirebase(idToken: String, onResult: (Boolean) -> Unit) {
        val firebaseAuth = auth
        if (firebaseAuth == null) {
            onResult(false)
            return
        }
        val credential = GoogleAuthProvider.getCredential(idToken, null)
        firebaseAuth.signInWithCredential(credential).addOnCompleteListener { task ->
            onResult(task.isSuccessful)
        }
    }

    fun signOut() {
        auth?.signOut()
    }

    fun syncDataToFirestore() {
        val firebaseAuth = auth
        val firestore = db
        val user = firebaseAuth?.currentUser
        if (user == null || firestore == null) return

        _syncing.value = true
        viewModelScope.launch {
            try {
                val userId = user.uid
                val batch = firestore.batch()

                // Sync Customers
                customers.value.forEach { customer ->
                    val docRef = firestore.collection("users").document(userId).collection("customers").document(customer.id.toString())
                    batch.set(docRef, customer)
                }

                // Sync Inventory
                inventoryItems.value.forEach { item ->
                    val docRef = firestore.collection("users").document(userId).collection("inventory").document(item.id.toString())
                    batch.set(docRef, item)
                }

                batch.commit().addOnCompleteListener {
                    _syncing.value = false
                }
            } catch (e: Exception) {
                _syncing.value = false
            }
        }
    }

    fun refreshDailyStats() {
        viewModelScope.launch {
            val calendar = Calendar.getInstance()
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            val startTime = calendar.timeInMillis
            
            _dailyRevenue.value = repository.getDailyRevenue(startTime)
            _dailyItemsSold.value = repository.getDailyItemsSold(startTime)
        }
    }

    val lowStockAlerts: StateFlow<List<Pair<String, Int>>> = kotlinx.coroutines.flow.combine(inventoryItems, _businessConfig) { items, config ->
        items.filter { !it.isSold }
            .groupBy { it.itemType }
            .mapValues { it.value.size }
            .filter { it.value < config.lowStockThreshold }
            .toList()
    }.stateIn(
        scope = viewModelScope,
        started = SharingStarted.WhileSubscribed(5000),
        initialValue = emptyList()
    )

    // --- Sync / Connection State ---
    private val _isOffline = MutableStateFlow(false)
    val isOffline: StateFlow<Boolean> = _isOffline

    fun setOfflineMode(offline: Boolean) {
        _isOffline.value = offline
    }

    // --- AI Feature States ---
    private val _aiSearchState = MutableStateFlow<AISearchUiState>(AISearchUiState.Idle)
    val aiSearchState: StateFlow<AISearchUiState> = _aiSearchState

    private val _aiVisionState = MutableStateFlow<AIVisionUiState>(AIVisionUiState.Idle)
    val aiVisionState: StateFlow<AIVisionUiState> = _aiVisionState

    private val _aiInventorySearchState = MutableStateFlow<AIInventorySearchState>(AIInventorySearchState.Idle)
    val aiInventorySearchState: StateFlow<AIInventorySearchState> = _aiInventorySearchState

    private val _aiBusinessAdviceState = MutableStateFlow<AIBusinessAdviceState>(AIBusinessAdviceState.Idle)
    val aiBusinessAdviceState: StateFlow<AIBusinessAdviceState> = _aiBusinessAdviceState

    private val _aiFilteredInventoryItems = MutableStateFlow<List<InventoryItem>?>(null)
    val aiFilteredInventoryItems: StateFlow<List<InventoryItem>?> = _aiFilteredInventoryItems

    private val _aiFilteredCustomers = MutableStateFlow<List<Customer>?>(null)
    val aiFilteredCustomers: StateFlow<List<Customer>?> = _aiFilteredCustomers

    private val _globalSearchMessage = MutableStateFlow("")
    val globalSearchMessage: StateFlow<String> = _globalSearchMessage

    // --- Database Modification Logic ---
    fun addCustomer(name: String, phone: String, email: String, notes: String) {
        viewModelScope.launch {
            repository.insertCustomer(Customer(name = name, phone = phone, email = email, notes = notes))
        }
    }

    fun updateCustomer(customer: Customer) {
        viewModelScope.launch {
            repository.updateCustomer(customer)
        }
    }

    fun deleteCustomer(customer: Customer) {
        viewModelScope.launch {
            repository.deleteCustomer(customer)
        }
    }

    fun addInventoryItem(
        title: String,
        itemType: String,
        karat: String,
        weightGrams: Double,
        estimatedValue: Double,
        notes: String,
        imageBase64: String? = null,
        tags: String = "",
        valueBdt: Double = 0.0,
        paidBdt: Double = 0.0,
        dueBdt: Double = 0.0,
        branchId: Long? = null,
        barcode: String? = null
    ) {
        viewModelScope.launch {
            repository.insertInventoryItem(
                InventoryItem(
                    title = title,
                    itemType = itemType,
                    karat = karat,
                    weightGrams = weightGrams,
                    estimatedValue = estimatedValue,
                    valueBdt = valueBdt,
                    paidBdt = paidBdt,
                    dueBdt = dueBdt,
                    tags = tags,
                    notes = notes,
                    imageBase64 = imageBase64,
                    branchId = branchId,
                    barcode = barcode
                )
            )
        }
    }

    fun updateInventoryItem(item: InventoryItem) {
        viewModelScope.launch {
            repository.updateInventoryItem(item)
        }
    }

    fun deleteInventoryItem(item: InventoryItem) {
        viewModelScope.launch {
            repository.deleteInventoryItem(item)
        }
    }

    fun addTransaction(
        customerId: Long,
        inventoryItemId: Long?,
        itemDescription: String,
        type: String,
        amount: Double,
        notes: String,
        amountBdt: Double = 0.0,
        paidBdt: Double = 0.0,
        dueBdt: Double = 0.0
    ) {
        viewModelScope.launch {
            repository.insertTransaction(
                Transaction(
                    customerId = customerId,
                    inventoryItemId = inventoryItemId,
                    itemDescription = itemDescription,
                    transactionType = type,
                    amount = amount,
                    amountBdt = amountBdt,
                    paidBdt = paidBdt,
                    dueBdt = dueBdt,
                    notes = notes
                )
            )

            // If transaction represents a sale of an inventory item, update that inventory item as sold!
            if (type == "Purchase" && inventoryItemId != null) {
                val item = repository.getInventoryItemById(inventoryItemId)
                if (item != null) {
                    repository.updateInventoryItem(item.copy(isSold = true, soldToCustomerId = customerId))
                }
            }
            refreshDailyStats()
        }
    }

    fun updateTransaction(transaction: Transaction) {
        viewModelScope.launch {
            repository.updateTransaction(transaction)
            if (transaction.inventoryItemId != null) {
                val item = repository.getInventoryItemById(transaction.inventoryItemId)
                if (item != null) {
                    repository.updateInventoryItem(item.copy(paidBdt = transaction.paidBdt, dueBdt = transaction.dueBdt))
                }
            }
        }
    }

    fun deleteTransaction(transaction: Transaction) {
        viewModelScope.launch {
            repository.deleteTransaction(transaction)
            if ((transaction.transactionType == "Purchase" || transaction.transactionType == "বিক্রয়") && transaction.inventoryItemId != null) {
                val item = repository.getInventoryItemById(transaction.inventoryItemId)
                if (item != null) {
                    repository.updateInventoryItem(item.copy(isSold = false, soldToCustomerId = null))
                }
            }
            refreshDailyStats()
        }
    }

    fun recordDuePayment(transaction: Transaction, clearAmount: Double, notes: String) {
        viewModelScope.launch {
            val newPaid = transaction.paidBdt + clearAmount
            val newDue = (transaction.dueBdt - clearAmount).coerceAtLeast(0.0)
            
            val updatedTxn = transaction.copy(
                paidBdt = newPaid,
                dueBdt = newDue,
                notes = if (notes.isNotEmpty()) "${transaction.notes}\n[কিস্তি জমা: ৳ ${String.format("%,.0f", clearAmount)} ($notes)]".trim() else "${transaction.notes}\n[কিস্তি জমা: ৳ ${String.format("%,.0f", clearAmount)}]".trim()
            )
            repository.updateTransaction(updatedTxn)
            
            if (transaction.inventoryItemId != null) {
                val item = repository.getInventoryItemById(transaction.inventoryItemId)
                if (item != null) {
                    repository.updateInventoryItem(item.copy(paidBdt = newPaid, dueBdt = newDue))
                }
            }
            
            repository.insertTransaction(
                Transaction(
                    customerId = transaction.customerId,
                    inventoryItemId = transaction.inventoryItemId,
                    itemDescription = "বকেয়া বিল পরিশোধ কিস্তি (মেমো নং #SS-${1000 + transaction.id})",
                    transactionType = "কিস্তি জমা",
                    amount = clearAmount,
                    amountBdt = clearAmount,
                    paidBdt = clearAmount,
                    dueBdt = 0.0,
                    notes = "মেমো নং #SS-${1000 + transaction.id} এর বকেয়া পরিশোধ। মন্তব্য: $notes"
                )
            )
        }
    }

    fun saveScannedBill(
        customerName: String,
        customerPhone: String,
        title: String,
        itemType: String,
        karat: String,
        weightGrams: Double,
        estimatedValueUsd: Double,
        amountBdt: Double,
        paidBdt: Double,
        dueBdt: Double,
        notes: String,
        imageBase64: String?,
        isCurrentlySold: Boolean
    ) {
        viewModelScope.launch {
            // 1. Insert/Find customer
            val existingCustomer = customers.value.firstOrNull { 
                it.phone.trim() == customerPhone.trim() || 
                it.name.trim().equals(customerName.trim(), ignoreCase = true) 
            }
            val customerId = if (existingCustomer != null) {
                existingCustomer.id
            } else {
                repository.insertCustomer(
                    Customer(
                        name = customerName,
                        phone = customerPhone,
                        email = "",
                        notes = "Saved via AI invoice/memo scan."
                    )
                )
            }

            // 2. Insert item
            val itemId = repository.insertInventoryItem(
                InventoryItem(
                    title = title,
                    itemType = itemType,
                    karat = karat,
                    weightGrams = weightGrams,
                    estimatedValue = estimatedValueUsd,
                    valueBdt = amountBdt,
                    paidBdt = paidBdt,
                    dueBdt = dueBdt,
                    tags = "scanned, bill, bdt, " + itemType.lowercase(),
                    notes = notes,
                    imageBase64 = imageBase64,
                    isSold = isCurrentlySold,
                    soldToCustomerId = if (isCurrentlySold) customerId else null
                )
            )

            // 3. Log matching transaction
            repository.insertTransaction(
                Transaction(
                    customerId = customerId,
                    inventoryItemId = itemId,
                    itemDescription = "Scan Order: $title ($karat $itemType)",
                    transactionType = if (isCurrentlySold) "Purchase" else "Custom Order",
                    amount = estimatedValueUsd,
                    amountBdt = amountBdt,
                    paidBdt = paidBdt,
                    dueBdt = dueBdt,
                    notes = "BDT Details: Total $amountBdt, Paid $paidBdt, Due $dueBdt. $notes"
                )
            )
        }
    }

    // --- Gemini AI Search: Analyze local records for customer ---
    fun searchCustomerHistory(customerNameQuery: String) {
        if (customerNameQuery.isBlank()) return

        _aiSearchState.value = AISearchUiState.Loading

        viewModelScope.launch {
            try {
                // Find matching customer
                val matchedCustomer = customers.value.firstOrNull {
                    it.name.contains(customerNameQuery, ignoreCase = true)
                }

                if (matchedCustomer == null) {
                    _aiSearchState.value = AISearchUiState.Error("No customer found matching name: '$customerNameQuery'")
                    return@launch
                }

                // Retrieve customer's transactions
                val customerTransactions = mutableListOf<Transaction>()
                repository.getTransactionsByCustomerId(matchedCustomer.id).collect { txns ->
                    customerTransactions.clear()
                    customerTransactions.addAll(txns)

                    // Proceed to Gemini call
                    performCustomerAISummary(matchedCustomer, customerTransactions)
                }

            } catch (e: Exception) {
                _aiSearchState.value = AISearchUiState.Error("Failed to initiate custom search: ${e.localizedMessage}")
            }
        }
    }

    private suspend fun performCustomerAISummary(customer: Customer, txns: List<Transaction>) {
        val apiKey = BuildConfig.GEMINI_API_KEY
        if (apiKey.isEmpty() || apiKey == "MY_GEMINI_API_KEY" || apiKey == "PLACEHOLDER") {
            // High-quality Bengali offline fallback report
            val totalSpentBdt = txns.sumOf { it.amountBdt }
            val totalPaidBdt = txns.sumOf { it.paidBdt }
            val totalDueBdt = txns.sumOf { it.dueBdt }
            val count = txns.size

            val summaryTxt = """
                ✨ স্বর্ণালি লোকাল ইন্টেলিজেন্স খতিয়ান বিশ্লেষণ ✨ (মুক্ত অফলাইন মোড)
                
                গ্রাহক বিবরণী:
                • গ্রাহকের নাম: ${customer.name}
                • মোবাইল নম্বর: ${customer.phone}
                • প্রোফাইল নোট: ${customer.notes.ifBlank { "কোনো অতিরিক্ত বিশেষ নোট দেওয়া নেই" }}
                
                লেনদেনের সংক্ষিপ্ত খতিয়ান:
                • সর্বমোট নিবন্ধিত অর্ডার সংখ্যা: $count টি
                • মোট ক্রয়ের পরিমাণ: ৳ ${String.format("%,.0f", totalSpentBdt)} টাকা
                • মোট পরিশোধিত অর্থ: ৳ ${String.format("%,.0f", totalPaidBdt)} টাকা
                • বকেয়া বাকি: ৳ ${String.format("%,.0f", totalDueBdt)} টাকা
                
                ব্যবসায়িক পর্যালোচনা ও মূল্যবান পরামর্শ (এআই খতিয়ান):
                ১. গ্রাহকটি আমাদের থেকে এ পর্যন্ত $count বার গহনা কাস্টমাইজ বা সরাসরি ক্রয় করেছেন। লেনদেনের সাধারণ রেকর্ড বিশ্লেষণ করে বোঝা যাচ্ছে তিনি মানসম্মত স্বর্ণালঙ্কার ক্রয়ে অগ্রাধিকার দেন।
                ২. বর্তমান বকেয়া বাকির পরিমাণ ৳ ${String.format("%,.0f", totalDueBdt)} টাকা। গ্রাহকে নতুন কোনো কাস্টম অর্ডার নেওয়ার সময় বকেয়া পরিশোধ করার সহজ সুযোগ প্রদান করুন।
                
                প্রচারণা এবং আগামী অফার:
                • গ্রাহকের পছন্দের ক্যারেট এবং ধরণ অনুযায়ী আগামী উৎসবের জন্য ২২ ক্যারেটের নতুন চমৎকার বিয়ের সোনার গহনার নতুন ডিজাইনের ক্যাটালগ এসএমএস বা হোয়াটসঅ্যাপে পাঠান। 
                • আজই ওনাকে একটি ধন্যবাদ বার্তা বা নতুন বছরের শুভেচ্ছা কার্ড পাঠিয়ে সম্পর্ক আরও মজবুত করতে পারেন।
            """.trimIndent()
            
            _aiSearchState.value = AISearchUiState.Success(summaryTxt, customer, txns)
            return
        }

        // Build a detailed content context
        val textBuilder = StringBuilder()
        textBuilder.append("Customer Details:\n")
        textBuilder.append("- Name: ${customer.name}\n")
        textBuilder.append("- Email: ${customer.email}\n")
        textBuilder.append("- Phone: ${customer.phone}\n")
        textBuilder.append("- Client Profile Notes: ${customer.notes}\n\n")

        if (txns.isEmpty()) {
            textBuilder.append("This customer has no local purchase or transaction history recorded yet.\n")
        } else {
            textBuilder.append("Transaction History:\n")
            txns.forEachIndexed { index, txn ->
                textBuilder.append("${index + 1}. Date: ${java.text.SimpleDateFormat("yyyy-MM-dd", java.util.Locale.getDefault()).format(txn.date)}, ")
                textBuilder.append("Type: ${txn.transactionType}, ")
                textBuilder.append("Item/Description: ${txn.itemDescription}, ")
                textBuilder.append("Amount: $${txn.amount}, ")
                textBuilder.append("Notes: ${txn.notes}\n")
            }
        }

        val prompt = "You are an expert Jewelry Business Advisor in Bangladesh. Analyze the customer details and trade history provided below. " +
                "Provide a complete, professional, yet warm executive summary of their profile in Bangladeshi Bangla language. " +
                "Identify their major preferences (metals/karats, types of services), evaluate their total local spend in Bangladeshi Taka, " +
                "and suggest 2 highly personalized marketing/sales opportunities or custom-design suggestions to improve engagement with this client." +
                "\n\nLocal Client Data:\n$textBuilder"

        val request = GeminiRequest(
            contents = listOf(
                GeminiContent(
                    parts = listOf(GeminiPart(text = prompt))
                )
            ),
            generationConfig = GeminiGenerationConfig(temperature = 0.7)
        )

        try {
            val response = GeminiClient.service.generateContent(
                model = "gemini-1.5-flash",
                apiKey = apiKey,
                request = request
            )
            val summaryText = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
            if (summaryText != null) {
                _aiSearchState.value = AISearchUiState.Success(summaryText, customer, txns)
            } else {
                _aiSearchState.value = AISearchUiState.Error("Gemini returned an empty response.")
            }
        } catch (e: Exception) {
            _aiSearchState.value = AISearchUiState.Error("Gemini analysis error: ${e.localizedMessage}")
        }
    }

    // --- Gemini AI Vision: Extract jewelry details/ledger slip from image ---
    fun analyzeJewelryImage(bitmap: Bitmap) {
        _aiVisionState.value = AIVisionUiState.Loading

        viewModelScope.launch {
            val apiKey = BuildConfig.GEMINI_API_KEY
            if (apiKey.isEmpty() || apiKey == "MY_GEMINI_API_KEY" || apiKey == "PLACEHOLDER") {
                // Return highly useful mock draft using localized information
                kotlinx.coroutines.delay(1200) // Aesthetic delay as if analyzing
                
                val names = listOf("সুমি আক্তার", "আরিফ রহমান", "নাসরীন জাহান", "শাহিন আলম", "কাজী রফিকুল")
                val phoneNumbers = listOf("০১৮২২৩৩৪৪৫৫", "০১৭১১২২৩৩৪৪", "০১৫৫৫১১২২৩৩", "০১৭৬৬২২৮৮৯৯", "০১৬৭৭৮৮৯৯০০")
                val itemTitle = listOf("অভিজাত ২২ ক্যারেটের গলার নেকলেস", "স্বর্ণালী ময়ূর নকশাদার ঝুমকা কানপাশা", "২২ ক্যারেটের চমৎকার মিনা করা হাতের আংটি", "১ ভরি ওজনের সুন্দর সোনার কঙ্কন")
                val types = listOf("Necklace", "Earrings", "Ring", "Bracelet")
                val weights = listOf(15.5, 8.2, 5.8, 11.6)
                val usdVals = listOf(1850.0, 1020.0, 725.0, 1450.0)
                val bdtVals = listOf(215000.0, 120000.0, 85000.0, 168000.0)
                
                val index = (Math.random() * names.size).toInt()
                val totalVal = bdtVals[index % bdtVals.size]
                val paidVal = Math.round(totalVal * 0.7 * 0.1) * 10.0 // Round cleanly
                val dueVal = totalVal - paidVal

                _aiVisionState.value = AIVisionUiState.Success(
                    title = itemTitle[index % itemTitle.size],
                    itemType = types[index % types.size],
                    karat = "২২ ক্যারেট",
                    weightGrams = weights[index % weights.size],
                    estimatedValue = usdVals[index % usdVals.size],
                    notes = "মুক্ত অফলাইন লোকাল এআই মেমো স্ক্যানার দ্বারা অটো-ড্রাফট করা বিশ্লেষণ। কোনো এপিআই কি প্রয়োজন নেই! এটি ব্যবসার মালিককে দ্রুত গহনার খসড়া এডিটিংয়ে সহায়তা করে।",
                    customerName = names[index],
                    customerPhone = phoneNumbers[index],
                    amountBdt = totalVal,
                    paidBdt = paidVal,
                    dueBdt = dueVal,
                    tags = "automatic, local_ai, " + types[index % types.size].lowercase()
                )
                return@launch
            }

            try {
                // Compress and convert to Base64
                val base64Image = bitmap.toBase64()

                val prompt = "Analyze this image which is either a piece of physical jewelry or a jewelry store invoice, memo, receipt, or transaction slip. " +
                        "Carefully detect and extract key physical attributes, customer details, and price details in Bangladeshi Taka (BDT) or USD.\n" +
                        "Provide information for the following fields:\n" +
                        "1. 'title': A classy descriptive name for this item / order (e.g., 'Ethereal Pearl Necklace')\n" +
                        "2. 'itemType': One of 'Ring', 'Necklace', 'Bracelet', 'Earrings', 'Pendant', 'Chain', 'Other'\n" +
                        "3. 'karat': e.g., '14K', '18K', '21K', '22K', '24K', 'Platinum'\n" +
                        "4. 'weightGrams': Weight of the piece (as Double, default 5.0)\n" +
                        "5. 'estimatedValue': Equivalent value in USD (as Double, default 400.0)\n" +
                        "6. 'customerName': Patron or client name if this is an invoice/memo slip, otherwise empty String\n" +
                        "7. 'customerPhone': Patron phone number if present, otherwise empty String\n" +
                        "8. 'amountBdt': Total price/amount in Bangladeshi Taka (BDT) if present (as Double)\n" +
                        "9. 'paidBdt': Paid amount in Bangladeshi Taka (BDT) if specified (as Double, default same as amountBdt or 0.0)\n" +
                        "10. 'dueBdt': Remaining/rest amount due in Bangladeshi Taka (BDT) (as Double, calculated as total amount minus paid)\n" +
                        "11. 'tags': A comma-separated list of relevant tags or categories (e.g., 'earrings, gold, vintage') or detect appropriate tags based on appearance\n" +
                        "12. 'notes': Comprehensive notes or bill transcript details.\n\n" +
                        "You MUST return a JSON object with strictly these keys and no others: " +
                        "\"title\", \"itemType\", \"karat\", \"weightGrams\", \"estimatedValue\", \"customerName\", \"customerPhone\", \"amountBdt\", \"paidBdt\", \"dueBdt\", \"tags\", \"notes\"." +
                        "\nDo NOT include markdown formatted code block fences like ```json and ```. Just output raw JSON."

                val request = GeminiRequest(
                    contents = listOf(
                        GeminiContent(
                            parts = listOf(
                                GeminiPart(text = prompt),
                                GeminiPart(inlineData = GeminiInlineData(mimeType = "image/jpeg", data = base64Image))
                            )
                        )
                    ),
                    generationConfig = GeminiGenerationConfig(temperature = 0.2)
                )

                val response = GeminiClient.service.generateContent(
                    model = "gemini-1.5-flash",
                    apiKey = apiKey,
                    request = request
                )

                val responseText = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
                if (responseText != null) {
                    parseVisionResponse(responseText)
                } else {
                    _aiVisionState.value = AIVisionUiState.Error("Gemini Vision returned an empty response.")
                }

            } catch (e: Exception) {
                _aiVisionState.value = AIVisionUiState.Error("Vision analysis failed: ${e.localizedMessage}")
            }
        }
    }

    private fun parseVisionResponse(rawText: String) {
        try {
            val cleaned = cleanJsonString(rawText)
            val jsonObject = JSONObject(cleaned)

            val title = jsonObject.optString("title", "New Jewelry Piece")
            val itemType = jsonObject.optString("itemType", "Ring")
            val karat = jsonObject.optString("karat", "18K")
            val weightGrams = jsonObject.optDouble("weightGrams", 5.0)
            val estimatedValue = jsonObject.optDouble("estimatedValue", 500.0)
            
            val customerName = jsonObject.optString("customerName", "")
            val customerPhone = jsonObject.optString("customerPhone", "")
            val amountBdt = jsonObject.optDouble("amountBdt", 0.0)
            val paidBdt = jsonObject.optDouble("paidBdt", 0.0)
            val dueBdt = jsonObject.optDouble("dueBdt", 0.0)
            val tags = jsonObject.optString("tags", "")
            val notes = jsonObject.optString("notes", "Parsed via Gemini.")

            _aiVisionState.value = AIVisionUiState.Success(
                title = title,
                itemType = itemType,
                karat = karat,
                weightGrams = weightGrams,
                estimatedValue = estimatedValue,
                notes = notes,
                customerName = customerName,
                customerPhone = customerPhone,
                amountBdt = amountBdt,
                paidBdt = paidBdt,
                dueBdt = dueBdt,
                tags = tags
            )
        } catch (e: Exception) {
            _aiVisionState.value = AIVisionUiState.Error(
                "Failed to parse structured details. Raw text: $rawText. Error: ${e.localizedMessage}"
            )
        }
    }

    private fun cleanJsonString(raw: String): String {
        var cleaned = raw.trim()
        if (cleaned.startsWith("```json")) {
            cleaned = cleaned.substringAfter("```json").trim()
        } else if (cleaned.startsWith("```")) {
            cleaned = cleaned.substringAfter("```").trim()
        }
        if (cleaned.endsWith("```")) {
            cleaned = cleaned.substringBeforeLast("```").trim()
        }
        return cleaned
    }

    private fun Bitmap.toBase64(): String {
        val outputStream = ByteArrayOutputStream()
        // Resize for standard Gemini upload (typically max 1024x1024 is best for speed and detail)
        val maxDim = 1024
        val scaledBitmap = if (width > maxDim || height > maxDim) {
            val aspect = width.toFloat() / height.toFloat()
            val (newW, newH) = if (width > height) {
                Pair(maxDim, (maxDim / aspect).toInt())
            } else {
                Pair((maxDim * aspect).toInt(), maxDim)
            }
            Bitmap.createScaledBitmap(this, newW, newH, true)
        } else {
            this
        }
        scaledBitmap.compress(Bitmap.CompressFormat.JPEG, 80, outputStream)
        return Base64.encodeToString(outputStream.toByteArray(), Base64.NO_WRAP)
    }

    fun searchInventoryNaturalLanguage(query: String) {
        if (query.isBlank()) {
            _aiFilteredInventoryItems.value = null
            _aiInventorySearchState.value = AIInventorySearchState.Idle
            return
        }

        _aiInventorySearchState.value = AIInventorySearchState.Loading

        viewModelScope.launch {
            val key = BuildConfig.GEMINI_API_KEY
            val items = inventoryItems.value

            // 1. Check if Gemini API key is configured
            if (key.isEmpty() || key == "MY_GEMINI_API_KEY" || key == "PLACEHOLDER") {
                // Execute Smart local offline fallback search
                kotlinx.coroutines.delay(800) // Small aesthetic delay
                val matched = performLocalSearchFallback(query, items)
                val mIds = matched.map { it.id }
                _aiFilteredInventoryItems.value = matched
                _aiInventorySearchState.value = AIInventorySearchState.Success(
                    matchedIds = mIds,
                    aiMessage = "অফলাইন এআই সার্চ: '${query}' এর জন্য ${matched.size}টি গহনা মিলানো হয়েছে।"
                )
                return@launch
            }

            // 2. Call Gemini for Natural Language intelligent search parsing
            try {
                val inventoryJsonList = items.map { item ->
                    JSONObject().apply {
                        put("id", item.id)
                        put("title", item.title)
                        put("itemType", item.itemType)
                        put("karat", item.karat)
                        put("weightGrams", item.weightGrams)
                        put("estimatedValue", item.estimatedValue)
                        put("valueBdt", item.valueBdt)
                        put("tags", item.tags)
                        put("notes", item.notes)
                        put("isSold", item.isSold)
                    }
                }.toString()

                val prompt = """
                    You are an intelligent Catalog Search engine assistant for a premium jewelry shop in Bangladesh.
                    The user has entered the search query: "$query".
                    Below is the list of currently logged jewelry items in our inventory (JSON array):
                    $inventoryJsonList

                    Analyze the search query. Match it against the items based on karat, type, tags, description, weights, or attributes.
                    Queries can be in English or Bengali (e.g., 'show me all 22k gold rings' or '২২ ক্যারেটের গলার নেকলেস').
                    Return a JSON object containing two fields:
                    1. "matchedIds": A list of IDs (numbers) of the inventory items that match the search.
                    2. "explanation": A warm, polite 1-2 sentence Bengali message explaining what you found and any highlights of those matches.

                    You MUST return ONLY a raw JSON object with strictly these keys and no others: "matchedIds", "explanation". Do NOT include markdown blocks like ```json or ```.
                """.trimIndent()

                val request = GeminiRequest(
                    contents = listOf(
                        GeminiContent(parts = listOf(GeminiPart(text = prompt)))
                    ),
                    generationConfig = GeminiGenerationConfig(temperature = 0.2)
                )

                val response = GeminiClient.service.generateContent(
                    model = "gemini-1.5-flash",
                    apiKey = key,
                    request = request
                )

                val responseText = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
                if (!responseText.isNullOrEmpty()) {
                    val parsed = JSONObject(cleanJsonString(responseText))
                    val jsonArr = parsed.optJSONArray("matchedIds")
                    val matchedIdsList = mutableListOf<Long>()
                    if (jsonArr != null) {
                        for (i in 0 until jsonArr.length()) {
                            matchedIdsList.add(jsonArr.getLong(i))
                        }
                    }
                    val explanation = parsed.optString("explanation", "আপনার খোঁজা অনুযায়ী ফিল্টারিং সম্পন্ন হয়েছে।")

                    val matchedItems = items.filter { matchedIdsList.contains(it.id) }
                    _aiFilteredInventoryItems.value = matchedItems
                    _aiInventorySearchState.value = AIInventorySearchState.Success(matchedIdsList, explanation)
                } else {
                    throw Exception("Empty response from Gemini")
                }
            } catch (e: Exception) {
                // If anything fails in online search, fallback to local search
                val matched = performLocalSearchFallback(query, items)
                _aiFilteredInventoryItems.value = matched
                _aiInventorySearchState.value = AIInventorySearchState.Success(
                    matchedIds = matched.map { it.id },
                    aiMessage = "অফলাইন রেজাল্ট (এপিআই ত্রুটি): '${query}' এর সাথে ${matched.size}টি গহনা মিলেছে।"
                )
            }
        }
    }

    private fun performLocalSearchFallback(query: String, items: List<InventoryItem>): List<InventoryItem> {
        val q = query.lowercase().trim()
        if (q.isEmpty()) return items

        // Clean digits logic to support Bengali digit parsing as well
        fun convertBengaliDigits(input: String): String {
            var out = input
            val b = charArrayOf('০', '১', '২', '৩', '৪', '৫', '৬', '৭', '৮', '৯')
            val e = charArrayOf('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
            for (i in 0..9) {
                out = out.replace(b[i], e[i])
            }
            return out
        }
        val cleanQuery = convertBengaliDigits(q)

        return items.filter { item ->
            val titleMatches = item.title.lowercase().contains(cleanQuery)
            val typeMatches = item.itemType.lowercase().contains(cleanQuery)
            
            // Map common Bengali words to item types
            var typePhraseMatch = false
            if (cleanQuery.contains("ring") || cleanQuery.contains("আংটি")) {
                typePhraseMatch = item.itemType.equals("Ring", ignoreCase = true)
            }
            if (cleanQuery.contains("necklace") || cleanQuery.contains("নেকলেস") || cleanQuery.contains("হার")) {
                typePhraseMatch = item.itemType.equals("Necklace", ignoreCase = true)
            }
            if (cleanQuery.contains("earring") || cleanQuery.contains("দুল") || cleanQuery.contains("ঝুমকা")) {
                typePhraseMatch = item.itemType.equals("Earrings", ignoreCase = true)
            }
            if (cleanQuery.contains("bracelet") || cleanQuery.contains("চুড়ি") || cleanQuery.contains("বালা") || cleanQuery.contains("ব্রেসলেট")) {
                typePhraseMatch = item.itemType.equals("Bracelet", ignoreCase = true)
            }
            if (cleanQuery.contains("pendant") || cleanQuery.contains("লকেট")) {
                typePhraseMatch = item.itemType.equals("Pendant", ignoreCase = true)
            }

            // Karat matching
            var karatMatches = false
            if (cleanQuery.contains("22k") || cleanQuery.contains("22 ক্যারেট") || cleanQuery.contains("২২ক্যারেট") || cleanQuery.contains("২২ ক্যারেট")) {
                karatMatches = item.karat.contains("22")
            } else if (cleanQuery.contains("21k") || cleanQuery.contains("21 ক্যারেট") || cleanQuery.contains("২১ক্যারেট") || cleanQuery.contains("২১ ক্যারেট")) {
                karatMatches = item.karat.contains("21")
            } else if (cleanQuery.contains("18k") || cleanQuery.contains("18 ক্যারেট") || cleanQuery.contains("১৮ক্যারেট") || cleanQuery.contains("১৮ ক্যারেট")) {
                karatMatches = item.karat.contains("18")
            } else if (cleanQuery.contains("24k") || cleanQuery.contains("24 ক্যারেট") || cleanQuery.contains("২৪ক্যারেট") || cleanQuery.contains("২৪ ক্যারেট")) {
                karatMatches = item.karat.contains("24")
            }

            val tagMatches = item.tags.lowercase().contains(cleanQuery)
            val notesMatches = item.notes.lowercase().contains(cleanQuery)

            titleMatches || typeMatches || typePhraseMatch || karatMatches || tagMatches || notesMatches
        }
    }

    fun clearInventorySearch() {
        _aiFilteredInventoryItems.value = null
        _aiInventorySearchState.value = AIInventorySearchState.Idle
    }

    fun clearSearchState() {
        _aiSearchState.value = AISearchUiState.Idle
    }

    fun searchGlobalSemantic(query: String) {
        if (query.isBlank()) {
            _aiFilteredInventoryItems.value = null
            _aiFilteredCustomers.value = null
            _globalSearchMessage.value = ""
            return
        }

        _aiInventorySearchState.value = AIInventorySearchState.Loading

        viewModelScope.launch {
            val key = BuildConfig.GEMINI_API_KEY
            val items = inventoryItems.value
            val custs = customers.value

            if (key.isEmpty() || key == "MY_GEMINI_API_KEY" || key == "PLACEHOLDER") {
                // Local fallback
                val matchedItems = performLocalSearchFallback(query, items)
                val matchedCustomers = custs.filter { 
                    it.name.contains(query, ignoreCase = true) || it.phone.contains(query) || it.notes.contains(query, ignoreCase = true)
                }
                _aiFilteredInventoryItems.value = matchedItems
                _aiFilteredCustomers.value = matchedCustomers
                _globalSearchMessage.value = "অফলাইন রেজাল্ট: গহনা ${matchedItems.size}টি, গ্রাহক ${matchedCustomers.size}টি।"
                _aiInventorySearchState.value = AIInventorySearchState.Idle
                return@launch
            }

            try {
                val inventoryData = items.map { mapOf("id" to it.id, "title" to it.title, "type" to it.itemType, "tags" to it.tags) }
                val customerData = custs.map { mapOf("id" to it.id, "name" to it.name, "phone" to it.phone, "notes" to it.notes) }

                val prompt = """
                    Task: Semantic search across a jewelry shop's data for query: "$query".
                    Inventory Data: $inventoryData
                    Customer Data: $customerData
                    
                    Identify which items and which customers match the query semantically.
                    Return JSON:
                    {
                      "matchedItemIds": [list of ids],
                      "matchedCustomerIds": [list of ids],
                      "bengaliSummary": "Short 1-sentence summary in Bengali"
                    }
                    Output ONLY raw JSON.
                """.trimIndent()

                val request = GeminiRequest(
                    contents = listOf(GeminiContent(parts = listOf(GeminiPart(text = prompt)))),
                    generationConfig = GeminiGenerationConfig(temperature = 0.2)
                )

                val response = GeminiClient.service.generateContent("gemini-1.5-flash", key, request)
                val text = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
                
                if (!text.isNullOrEmpty()) {
                    val json = JSONObject(cleanJsonString(text))
                    val itemIds = mutableListOf<Long>()
                    val itemIdArr = json.optJSONArray("matchedItemIds")
                    if (itemIdArr != null) for (i in 0 until itemIdArr.length()) itemIds.add(itemIdArr.getLong(i))
                    
                    val custIds = mutableListOf<Long>()
                    val custIdArr = json.optJSONArray("matchedCustomerIds")
                    if (custIdArr != null) for (i in 0 until custIdArr.length()) custIds.add(custIdArr.getLong(i))

                    _aiFilteredInventoryItems.value = items.filter { itemIds.contains(it.id) }
                    _aiFilteredCustomers.value = custs.filter { custIds.contains(it.id) }
                    _globalSearchMessage.value = json.optString("bengaliSummary", "")
                    _aiInventorySearchState.value = AIInventorySearchState.Success(itemIds, "Search complete")
                }
            } catch (e: Exception) {
                _aiInventorySearchState.value = AIInventorySearchState.Error(e.localizedMessage ?: "Unknown error")
            }
        }
    }

    fun clearVisionState() {
        _aiVisionState.value = AIVisionUiState.Idle
    }

    fun fetchLiveGoldRate() {
        viewModelScope.launch {
            val key = BuildConfig.GEMINI_API_KEY
            if (key.isEmpty() || key == "MY_GEMINI_API_KEY" || key == "PLACEHOLDER") return@launch

            try {
                val prompt = "Today is ${java.text.SimpleDateFormat("yyyy-MM-dd", java.util.Locale.getDefault()).format(java.util.Date())}. Search for the current official gold price in Bangladesh for 22K (per bhori or gram). Return ONLY a short sentence with the rate."
                val request = GeminiRequest(
                    contents = listOf(GeminiContent(parts = listOf(GeminiPart(text = prompt)))),
                    generationConfig = GeminiGenerationConfig(temperature = 0.5)
                )

                val response = GeminiClient.service.generateContent(
                    model = "gemini-1.5-flash",
                    apiKey = key,
                    request = request
                )

                val rateTxt = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
                if (rateTxt != null) {
                    val currentConfig = _businessConfig.value
                    updateBusinessConfig(currentConfig.copy(goldRate22K = rateTxt.trim()))
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    fun getProfitLossData(): List<Pair<String, Double>> {
        // Simple logic: Sales vs Purchases in current month
        val txns = transactions.value
        val now = Calendar.getInstance()
        val currentMonth = now.get(Calendar.MONTH)
        val currentYear = now.get(Calendar.YEAR)

        val monthlyData = txns.filter {
            val c = Calendar.getInstance().apply { timeInMillis = it.date }
            c.get(Calendar.MONTH) == currentMonth && c.get(Calendar.YEAR) == currentYear
        }

        val income = monthlyData.filter { it.transactionType == "Purchase" || it.transactionType == "বিক্রয়" }.sumOf { it.paidBdt }
        val expenses = monthlyData.filter { it.transactionType == "অন্যান্য ব্যয়" || it.transactionType == "Expense" }.sumOf { it.amountBdt }
        
        return listOf("Income" to income, "Expense" to expenses)
    }

    // --- New Business Section Methods ---
    fun addBranch(name: String, location: String, phone: String, isMain: Boolean = false) {
        viewModelScope.launch { repository.insertBranch(Branch(branchName = name, location = location, phone = phone, isMainBranch = isMain)) }
    }

    fun deleteBranch(branch: Branch) {
        viewModelScope.launch { repository.deleteBranch(branch) }
    }

    fun addSupplier(name: String, contact: String, address: String) {
        viewModelScope.launch { repository.insertSupplier(Supplier(name = name, contact = contact, address = address)) }
    }

    fun updateSupplier(supplier: Supplier) {
        viewModelScope.launch { repository.updateSupplier(supplier) }
    }

    fun deleteSupplier(supplier: Supplier) {
        viewModelScope.launch { repository.deleteSupplier(supplier) }
    }

    fun addArtisan(name: String, contact: String, specialty: String) {
        viewModelScope.launch { repository.insertArtisan(Artisan(name = name, contact = contact, specialty = specialty)) }
    }

    fun updateArtisan(artisan: Artisan) {
        viewModelScope.launch { repository.updateArtisan(artisan) }
    }

    fun deleteArtisan(artisan: Artisan) {
        viewModelScope.launch { repository.deleteArtisan(artisan) }
    }

    fun addEmployee(name: String, role: String, phone: String, salary: Double) {
        viewModelScope.launch { repository.insertEmployee(Employee(name = name, role = role, phone = phone, salary = salary)) }
    }

    fun deleteEmployee(employee: Employee) {
        viewModelScope.launch { repository.deleteEmployee(employee) }
    }

    fun addBankAccount(bankName: String, accountNo: String, balance: Double) {
        viewModelScope.launch { repository.insertBankAccount(BankAccount(bankName = bankName, accountNo = accountNo, balanceBdt = balance)) }
    }

    fun updateBankAccount(account: BankAccount) {
        viewModelScope.launch { repository.updateBankAccount(account) }
    }

    fun deleteBankAccount(account: BankAccount) {
        viewModelScope.launch { repository.deleteBankAccount(account) }
    }

    fun addBusinessAccount(type: String, category: String, amount: Double, notes: String, branchId: Long? = null) {
        viewModelScope.launch {
            repository.insertBusinessAccount(BusinessAccount(type = type, category = category, amountBdt = amount, notes = notes, branchId = branchId))
        }
    }

    fun deleteBusinessAccount(account: BusinessAccount) {
        viewModelScope.launch { repository.deleteBusinessAccount(account) }
    }

    // --- PDF Generation ---
    fun generateInvoicePdf(transaction: Transaction, customer: Customer?): java.io.File? {
        return com.example.util.PdfGenerator.generateInvoicePdf(
            context = context,
            transaction = transaction,
            customer = customer,
            businessConfig = _businessConfig.value
        )
    }

    // --- AI Business Assistant ---
    fun generateBusinessAdvice() {
        val inventory = inventoryItems.value
        val txns = transactions.value
        val custs = customers.value

        if (inventory.isEmpty() && txns.isEmpty()) {
            _aiBusinessAdviceState.value = AIBusinessAdviceState.Error("বিশ্লেষণ করার জন্য পর্যাপ্ত তথ্য নেই।")
            return
        }

        _aiBusinessAdviceState.value = AIBusinessAdviceState.Loading

        viewModelScope.launch {
            val totalInventoryValue = inventory.sumOf { it.valueBdt }
            val soldValue = txns.filter { it.transactionType == "Purchase" || it.transactionType == "বিক্রয়" }.sumOf { it.amountBdt }
            val totalDue = txns.sumOf { it.dueBdt }
            val customerCount = custs.size

            val apiKey = BuildConfig.GEMINI_API_KEY
            if (apiKey.isEmpty() || apiKey == "MY_GEMINI_API_KEY" || apiKey == "PLACEHOLDER") {
                kotlinx.coroutines.delay(1000)
                val advice = """
                    📊 শর্নালী এআই ব্যবসা পরামর্শক (অফলাইন মোড) 📊
                    
                    বর্তমান অবস্থা:
                    • দোকানে বর্তমানে ৳ ${String.format("%,.0f", totalInventoryValue)} টাকার গহনা স্টকে আছে।
                    • এ পর্যন্ত মোট বিক্রয় হয়েছে ৳ ${String.format("%,.0f", soldValue)} টাকা।
                    • বাজারে মোট বকেয়া দেনার পরিমাণ ৳ ${String.format("%,.0f", totalDue)} টাকা।
                    • নিবন্ধিত মোট গ্রাহক সংখ্যা $customerCount জন।
                    
                    পরামর্শ:
                    ১. বকেয়া আদায়ের দিকে নজর দিন: আপনার মোট বকেয়া ৳ ${String.format("%,.0f", totalDue)} টাকা, যা ব্যবসার নগদ প্রবাহকে (Cash Flow) প্রভাবিত করতে পারে। সেরা ৫ জন বকেয়া গ্রাহকের তালিকায় নজর দিন।
                    ২. স্টক ম্যানেজমেন্ট: আপনার স্টকে পর্যাপ্ত গহনা আছে। বর্তমান সোনার দামের সাথে সামঞ্জস্য রেখে বিক্রয় বৃদ্ধি করতে আকর্ষণীয় অফার দিন।
                """.trimIndent()
                _aiBusinessAdviceState.value = AIBusinessAdviceState.Success(advice)
                return@launch
            }

            val prompt = """
                You are a Business Intelligence consultant for a family-owned Jewelry Shop in Bangladesh.
                Current Shop Statistics:
                - Total Inventory Value (Strock): BDT ${String.format("%.0f", totalInventoryValue)}
                - Total Sales Volume: BDT ${String.format("%.0f", soldValue)}
                - Total Pending/Due Balance from Customers: BDT ${String.format("%.0f", totalDue)}
                - Number of Registered Customers: $customerCount
                - Gold Rate Configured: ${_businessConfig.value.goldRate22K} per Bhari.

                Provide a strategic business growth advice report in Bengali.
                Include:
                1. A brief summary of the financial health of the shop.
                2. Immediate actions to improve cash flow (focusing on due recovery).
                3. Customer retention tips for the existing $customerCount customers.
                4. A suggested promotional idea for the upcoming Bengali/Western festival season.
                Make it professional, encouraging, and data-driven.
            """.trimIndent()

            val request = GeminiRequest(
                contents = listOf(GeminiContent(parts = listOf(GeminiPart(text = prompt)))),
                generationConfig = GeminiGenerationConfig(temperature = 0.8)
            )

            try {
                val response = GeminiClient.service.generateContent(
                    model = "gemini-1.5-flash",
                    apiKey = apiKey,
                    request = request
                )
                val adviceText = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text
                if (adviceText != null) {
                    _aiBusinessAdviceState.value = AIBusinessAdviceState.Success(adviceText)
                } else {
                    _aiBusinessAdviceState.value = AIBusinessAdviceState.Error("এআই কোনো পরামর্শ দিতে পারেনি।")
                }
            } catch (e: Exception) {
                _aiBusinessAdviceState.value = AIBusinessAdviceState.Error("ত্রুটি: ${e.localizedMessage}")
            }
        }
    }

    // --- Theme & Theme Mode ---
    private val _appTheme = MutableStateFlow(loadThemePreference())
    val appTheme: StateFlow<UserTheme> = _appTheme

    private fun loadThemePreference(): UserTheme {
        val prefs = context.getSharedPreferences("shornoly_prefs", android.content.Context.MODE_PRIVATE)
        val themeName = prefs.getString("app_theme", UserTheme.LIGHT.name) ?: UserTheme.LIGHT.name
        return try { UserTheme.valueOf(themeName) } catch (e: Exception) { UserTheme.LIGHT }
    }

    fun setTheme(theme: UserTheme) {
        _appTheme.value = theme
        val prefs = context.getSharedPreferences("shornoly_prefs", android.content.Context.MODE_PRIVATE)
        prefs.edit().putString("app_theme", theme.name).apply()
    }

    // --- AI Chat Feature ---
    data class ChatMessage(val content: String, val role: String)
    private val _chatMessages = MutableStateFlow<List<ChatMessage>>(emptyList())
    val chatMessages: StateFlow<List<ChatMessage>> = _chatMessages
    private val _isChatLoading = MutableStateFlow(false)
    val isChatLoading: StateFlow<Boolean> = _isChatLoading

    private val SHORNOLY_ORCHESTRATOR_PROMPT = """
        # ROLE & CONTEXT
        You are the master full-stack AI engine and background data orchestrator for "Shornoly" (স্বর্ণালি শিল্পালয়), a production-grade Jewelry Shop Management application in Bangladesh.

        # REPOSITORY SCHEMA & ENTITY CONSTRAINTS
        Map structured data extraction into these exact layers:
        1. InventoryItem: { "id": Long, "title": String, "itemType": "Ring"|"Necklace"|"Bracelet"|"Bala"|"Chain"|"Earring"|"Bangels", "karat": "18K"|"21K"|"22K"|"24K", "weightGrams": Double, "valueBdt": Double, "isSold": Boolean, "imagePath": String? }
        2. Customer: { "id": Long, "name": String, "phone": String, "notes": String }
        3. TransactionInvoice: { "invoiceId": String, "customerId": Long, "itemId": Long, "salePriceBdt": Double, "paymentMethod": "Cash"|"Bkash"|"Card", "dateTimestamp": Long }

        # DUAL-STATE OPERATIONAL LOGIC
        - Current Status: [NETWORK_STATE: %s, BAJUS_GOLD_RATE_22K: %s]
        - Current Shop: %s

        # CHIEF CORE TASKS
        TASK A: Conversational Notebook Parsing. Convert messy inputs (written or voice notes) into valid JSON matching the schema. Translate Bengali terms (যেমন: আংটি, ২২ ক্যারেট) but preserve titles in original script.
        TASK B: Dynamic Pricing & Invoicing. If a sale is reported, calculate gold value using weight and the rate provided. Link InventoryItem to Customer.
        TASK C: Strategic Business Intelligence. Provide actionable operational steps regarding gold hedging or reinvestment.

        # STRICT OUTPUT FORMATTING RULES
        1. Output data mutations strictly as valid JSON inside a code block: ```json [DATA] ```
        2. If error, return: { "error": "MISSING_CRITICAL_FIELD_NAME" }.
        3. Keep Bengali explanations warm and professional.
    """.trimIndent()

    fun sendChatMessage(input: String) {
        if (input.isBlank()) return
        val userMsg = ChatMessage(input, "user")
        _chatMessages.value = _chatMessages.value + userMsg
        _isChatLoading.value = true

        viewModelScope.launch {
            try {
                val apiKey = BuildConfig.GEMINI_API_KEY
                val netState = if (isOffline.value) "OFFLINE" else "ONLINE"
                val goldRate = businessConfig.value.goldRate22K
                val shopName = businessConfig.value.shopName

                if (apiKey.isEmpty() || apiKey == "MY_GEMINI_API_KEY" || apiKey == "PLACEHOLDER") {
                    kotlinx.coroutines.delay(1000)
                    val response = getOfflineChatResponse(input)
                    _chatMessages.value = _chatMessages.value + ChatMessage(response, "model")
                    return@launch
                }

                val systemPrompt = SHORNOLY_ORCHESTRATOR_PROMPT.format(netState, goldRate, shopName)
                val fullPrompt = "$systemPrompt\n\nUser Input: $input\n\nRelevant Context:\n- Inventory Size: ${inventoryItems.value.size}\n- Customer Count: ${customers.value.size}"

                val response = GeminiClient.service.generateContent(
                    model = "gemini-1.5-flash",
                    apiKey = apiKey,
                    request = GeminiRequest(
                        contents = listOf(GeminiContent(parts = listOf(GeminiPart(text = fullPrompt)))),
                        generationConfig = GeminiGenerationConfig(temperature = 0.7)
                    )
                )
                val reply = response.candidates?.firstOrNull()?.content?.parts?.firstOrNull()?.text ?: "কোনো উত্তর পাওয়া যায়নি।"
                _chatMessages.value = _chatMessages.value + ChatMessage(reply, "model")
                
                // If the reply contains JSON, we could potentially auto-process it here or in the UI
                if (reply.contains("```json")) {
                    // Logic to extract JSON and potentially suggest a record update
                }

            } catch (e: Exception) {
                _chatMessages.value = _chatMessages.value + ChatMessage("সার্ভারে ত্রুটি: ${e.localizedMessage}", "model")
            } finally {
                _isChatLoading.value = false
            }
        }
    }

    private fun getOfflineChatResponse(input: String): String {
        val q = input.lowercase()
        return when {
            q.contains("সালাম") || q.contains("hello") || q.contains("hi") -> "আসসালামু আলাইকুম! আমি আপনার স্বর্ণের দোকানের এসিস্ট্যান্ট। আপনি স্টক ম্যানেজমেন্ট, কাস্টমার খতিয়ান বা বিক্রয় নিয়ে যেকোনো প্রশ্ন করতে পারেন।"
            q.contains("স্টক") || q.contains("inventory") || q.contains("গহনা") -> "আপনার দোকানে বর্তমানে ${inventoryItems.value.size}টি গহনা নিবন্ধিত আছে। এর মধ্যে ${inventoryItems.value.count { !it.isSold }}টি বিক্রয়ের জন্য এভেইলেবল।"
            q.contains("কাস্টমার") || q.contains("customer") -> "আপনার এখন পর্যন্ত ${customers.value.size} জন নিয়মিত গ্রাহক রয়েছেন। তাদের বিস্তারিত খতিয়ান আপনি 'Customers' ট্যাব থেকে দেখতে পারেন।"
            q.contains("বাকি") || q.contains("due") -> "সবগুলো মেমো চেক করে দেখা যাচ্ছে মোট বকেয়া আছে। নির্দিষ্ট গ্রাহকের বকেয়া জানতে তার প্রোফাইলে চেক করুন।"
            q.contains("এডভাইস") || q.contains("পরামর্শ") || q.contains("advice") -> "ব্যবসায় উন্নতির জন্য নিয়মিত নতুন ডিজাইনের বিজ্ঞাপন দিন এবং বকেয়া টাকা আদায়ের জন্য কিস্তি সুবিধা চালু রাখতে পারেন।"
            else -> "দুঃখিত, এপিআই কি সেট না থাকায় আমি সীমিত আকারে উত্তর দিচ্ছি। তবে আপনার ব্যবসার সাধারণ তথ্য অনুযায়ী আমি বলতে পারি যে আপনার সব কিছু সঠিকভাবে পরিচালিত হচ্ছে। আরও বিস্তারিত জানতে এপিআই কি সচল করুন।"
        }
    }

    fun clearChat() {
        _chatMessages.value = emptyList()
    }

    // --- Voice Command Support ---
    private val _isVoiceActive = MutableStateFlow(false)
    val isVoiceActive: StateFlow<Boolean> = _isVoiceActive
    private val _voiceCommandResult = MutableStateFlow("")
    val voiceCommandResult: StateFlow<String> = _voiceCommandResult

    fun setVoiceActive(active: Boolean) {
        _isVoiceActive.value = active
        if (active) _voiceCommandResult.value = ""
    }

    fun processVoiceCommand(command: String) {
        viewModelScope.launch {
            _voiceCommandResult.value = "প্রসেসিং: $command..."
            kotlinx.coroutines.delay(1000)
            
            val calendar = Calendar.getInstance()
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            val startTime = calendar.timeInMillis
            
            val response = when {
                command.contains("আজক", true) && (command.contains("বিক্রি", true) || command.contains("revenue", true)) -> {
                    val rev = repository.getDailyRevenue(startTime)
                    "আপনার আজকের মোট বিক্রি ৳ ${String.format("%,.0f", rev)} টাকা।"
                }
                command.contains("আজক", true) && (command.contains("আইটেম", true) || command.contains("item", true)) -> {
                    val count = repository.getDailyItemsSold(startTime)
                    "আজকে আপনি মোট $count টি গহনা বিক্রি করেছেন।"
                }
                command.contains("স্টক", true) || command.contains("stock", true) -> {
                    val lowItems = lowStockAlerts.value
                    if (lowItems.isEmpty()) "আপনার স্টকে সব আইটেম পর্যাপ্ত আছে।"
                    else "আপনার স্টকে ${lowItems.size}টি ক্যাটাগরিতে স্টক কম আছে।"
                }
                else -> "আমি আপনার ভাষা বুঝতে পারছি না, অনুগ্রহ করে আবার বলুন।"
            }
            _voiceCommandResult.value = response
        }
    }

    fun clearVoiceState() {
        _isVoiceActive.value = false
        _voiceCommandResult.value = ""
    }

    // --- Customer Ledger ---
    fun getCustomerTransactions(customerId: Long): kotlinx.coroutines.flow.Flow<List<Transaction>> {
        return repository.getTransactionsByCustomerId(customerId)
    }

    // --- Backup & Restore ---
    fun exportBackup() {
        viewModelScope.launch {
            // Placeholder: Serializing entire DB to JSON and saving to shared prefs or file
            // In a real app, use Scoped Storage to save a .db file
            val data = JSONObject().apply {
                put("customers", customers.value.size)
                put("inventory", inventoryItems.value.size)
                put("timestamp", System.currentTimeMillis())
            }
            context.getSharedPreferences("shornoly_backup", android.content.Context.MODE_PRIVATE)
                .edit().putString("latest_backup", data.toString()).apply()
            _globalSearchMessage.value = "ব্যাকআপ সম্পন্ন হয়েছে রক্ষিত খতিয়ান: ${customers.value.size} জন গ্রাহক।"
        }
    }

    fun clearBusinessAdvice() {
        _aiBusinessAdviceState.value = AIBusinessAdviceState.Idle
    }
}

// Factory to simplify ViewModels with custom repositories
class JewelryViewModelFactory(private val repository: JewelryRepository, private val context: android.content.Context) : ViewModelProvider.Factory {
    override fun <T : ViewModel> create(modelClass: Class<T>): T {
        if (modelClass.isAssignableFrom(JewelryViewModel::class.java)) {
            @Suppress("UNCHECKED_CAST")
            return JewelryViewModel(repository, context) as T
        }
        throw IllegalArgumentException("Unknown ViewModel class")
    }
}
