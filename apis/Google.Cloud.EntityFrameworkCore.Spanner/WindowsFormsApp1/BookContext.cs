using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.EntityFrameworkCore;

namespace WindowsFormsApp1
{
    public partial class BookContext : DbContext
    {
        public virtual DbSet<Book> Books { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSpanner(@"Data Source=projects/spanneref/instances/myspanner/databases/mydatabase");
        }
    }

    public class Book
    {
        public Book()
        {
        }

        public long ID { get; set; }

        public string Name { get; set; }

        public string Author { get; set; }

        public string ISBN { get; set; }

        [Column(TypeName = "DATE")]
        public DateTime PublishDate { get; set; }
    }
}
