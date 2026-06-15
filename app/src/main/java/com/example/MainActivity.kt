package com.example

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Scaffold
import androidx.compose.ui.Modifier
import androidx.lifecycle.lifecycleScope
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import com.example.data.Customer
import com.example.data.InventoryItem
import com.example.data.JewelryDatabase
import com.example.data.JewelryRepository
import com.example.ui.DashboardScreen
import com.example.ui.JewelryViewModel
import com.example.ui.JewelryViewModelFactory
import com.example.ui.theme.MyApplicationTheme
import com.example.util.GoogleSignInHelper
import com.google.firebase.FirebaseApp
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch

class MainActivity : ComponentActivity() {
  override fun onCreate(savedInstanceState: Bundle?) {
    super.onCreate(savedInstanceState)
    try {
      FirebaseApp.initializeApp(this)
    } catch (e: Exception) {
      // Gracefully handle initialization failure (e.g., if resources are missing)
    }
    enableEdgeToEdge()

    val database = JewelryDatabase.getDatabase(applicationContext)
    val dao = database.jewelryDao()
    val repository = JewelryRepository(dao)
    val viewModel: JewelryViewModel by viewModels { JewelryViewModelFactory(repository, applicationContext) }

    // Prepopulate database on first launch for a beautiful, fully functional mock experience
    lifecycleScope.launch {
      try {
        val existingCustomers = dao.getAllCustomers().first()
        if (existingCustomers.isEmpty()) {
          // Prepopulate beautiful boutique customer entries
          val sumiId = dao.insertCustomer(
            Customer(
              name = "সুমি আক্তার",
              phone = "০১৮২২৩৩৪৪৫৫",
              email = "sumi@gmail.com",
              notes = "২২ ক্যারেট সনাতন ও কাস্টম গহনা খুবই পছন্দ করেন। নকশায় ভারী রাজকীয় ময়ূর ও পাথর বসানো ডিজাইনে দারুণ আগ্রহ।"
            )
          )
          val arifId = dao.insertCustomer(
            Customer(
              name = "আরিফ রহমান",
              phone = "০১৭১১২২৩৩৪৪",
              email = "arif.rahman@gmail.com",
              notes = "বিয়ের ব্রাইডাল সেট ও ভারী নেকলেসের জন্য প্রায়ই অর্ডার করেন। সোনা ও প্লাটিনাম উভয় ধাতুর কালেকশনে আগ্রহী।"
            )
          )

          // Prepopulate catalog inventory items
          val item1Id = dao.insertInventoryItem(
            InventoryItem(
              title = "অভিজাত স্বর্ণের রাজকীয় ময়ূর নেকলেস",
              itemType = "Necklace",
              karat = "২২ ক্যারেট",
              weightGrams = 18.5,
              estimatedValue = 1850.0,
              valueBdt = 215000.0,
              paidBdt = 150000.0,
              dueBdt = 65000.0,
              tags = "luxury, exclusive, 22k, necklace, ময়ূর",
              notes = "২২ ক্যারেটের খাঁটি হলমার্ক সোনা দিয়ে তৈরি কারুকাজ খচিত ঐতিহ্যবাহী ময়ূর থিমের ডিজাইন নেকলেস।"
            )
          )
          val item2Id = dao.insertInventoryItem(
            InventoryItem(
              title = "জাদুকরী ডায়মন্ড কাট ঝুমকা কানপাশা",
              itemType = "Earrings",
              karat = "২১ ক্যারেট",
              weightGrams = 8.2,
              estimatedValue = 1020.0,
              valueBdt = 120000.0,
              paidBdt = 120000.0,
              dueBdt = 0.0,
              tags = "earrings, bridal, classic, ঝুমকা",
              notes = "ঝলমলে ডায়মন্ড কাট নকশাযুক্ত বিয়ের রাজকীয় কানপাশা ঝুমকা। গ্লসি ফিনিশিং।"
            )
          )
          val item3Id = dao.insertInventoryItem(
            InventoryItem(
              title = "মিনা করা সোনার কারুকার্যময় আংটি",
              itemType = "Ring",
              karat = "২২ ক্যারেট",
              weightGrams = 5.8,
              estimatedValue = 725.0,
              valueBdt = 85000.0,
              paidBdt = 45000.0,
              dueBdt = 40000.0,
              tags = "ring, minakari, dailywear, আংটি",
              notes = "লাল ও সবুজ সুন্দর মিনা রঙের চোখ ধাঁধানো ট্র্যাডিশনাল ডিজাইন হাতের আংটি।"
            )
          )

          // Prepopulate matching ledger transactions
          dao.insertTransaction(
            com.example.data.Transaction(
              customerId = sumiId,
              inventoryItemId = item1Id,
              itemDescription = "ক্রয়: অভিজাত স্বর্ণের রাজকীয় ময়ূর নেকলেস",
              transactionType = "Purchase",
              amount = 1850.0,
              amountBdt = 215000.0,
              paidBdt = 150000.0,
              dueBdt = 65000.0,
              notes = "পরিশোধিত ১৫০,০০০ টাকা নগদ, বাকি টাকা ১ মাসের মধ্যে দেওয়ার প্রতিশ্রুতি। হলমার্ক চেককৃত।"
            )
          )

          // Explicitly mark item1 as sold
          dao.insertInventoryItem(
            InventoryItem(
              id = item1Id,
              title = "অভিজাত স্বর্ণের রাজকীয় ময়ূর নেকলেস",
              itemType = "Necklace",
              karat = "২২ ক্যারেট",
              weightGrams = 18.5,
              estimatedValue = 1850.0,
              valueBdt = 215000.0,
              paidBdt = 150000.0,
              dueBdt = 65000.0,
              tags = "luxury, exclusive, 22k, necklace, ময়ূর",
              notes = "২২ ক্যারেটের খাঁটি হলমার্ক সোনা দিয়ে তৈরি কারুকাজ খচিত ঐতিহ্যবাহী ময়ূর থিমের ডিজাইন নেকলেস।।",
              isSold = true,
              soldToCustomerId = sumiId
            )
          )
        }
      } catch (e: Exception) {
        // Avoid crash in test runs
      }
    }

    setContent {
      val appTheme by viewModel.appTheme.collectAsState()
      MyApplicationTheme(theme = appTheme) {
        Scaffold(modifier = Modifier.fillMaxSize()) { innerPadding ->
          DashboardScreen(
            viewModel = viewModel,
            modifier = Modifier.padding(innerPadding)
          )
        }
      }
    }
  }
}
