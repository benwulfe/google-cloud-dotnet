using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace WindowsFormsApp1
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private async void button1_Click(object sender, EventArgs e)
        {
            using (var db = new BookContext())
            {
                var maxId = await db.Books.MaxAsync(u => (int?)u.ID);

                await db.Books.AddRangeAsync(new Book
                {
                    Author = "Ben",
                    ID = maxId.GetValueOrDefault(0) + 1,
                    ISBN = "ISBN",
                    Name = "How to add an item to EF",
                    PublishDate = DateTime.Now
                }, new Book
                {
                    Author = "Ben2",
                    ID = maxId.GetValueOrDefault(0) + 2,
                    ISBN = "ISBN2",
                    Name = "How to add an item to EF2",
                    PublishDate = DateTime.Now
                });

                db.SaveChanges(true);
                var newMaxId = await db.Books.MaxAsync(u => (int?)u.ID);

                if (newMaxId - maxId == 2)
                {
                    Console.WriteLine(@"It works!");
                }
            }

        }
    }
}
